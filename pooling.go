package dbox

import (
	"context"
	"sync"
	"time"

	"github.com/eaciit/toolkit"
)

// DbPooling is database pooling system in dbflex
type DbPooling struct {
	sync.RWMutex
	size  int
	items []*PoolItem
	fnNew func() (IConnection, error)

	// Timeout max time required to obtain new connection
	Timeout time.Duration

	// AutoRelease defines max time for a connection to be auto released after it is being idle. 0 = no autorelease (default)
	AutoRelease time.Duration

	// AutoClose defines max time for a connection to be autoclosed after it is being idle. 0 = no auto close (default)
	AutoClose time.Duration
}

// PoolItem is Item in the pool
type PoolItem struct {
	sync.RWMutex
	conn   IConnection
	used   bool
	closed bool

	lastUsed time.Time

	AutoRelease time.Duration
	AutoClose   time.Duration
}

// NewDbPooling create new pooling with given size
func NewDbPooling(size int, fnNew func() (IConnection, error)) *DbPooling {
	dbp := new(DbPooling)
	dbp.size = size
	dbp.fnNew = fnNew
	dbp.Timeout = time.Second * 2
	return dbp
}

// Get new connection. If all connection is being used and number of connection is less than
// pool capacity, new connection will be spin off. If capabity has been max out. It will waiting for
// any connection to be released before timeout reach
func (p *DbPooling) Get() (*PoolItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.Timeout)
	defer cancel()

	cpi := make(chan *PoolItem)
	cerr := make(chan error)

	//--- remove closed pi
	bufferItems := []*PoolItem{}
	for _, pi := range p.items {
		if !pi.isClosed() {
			bufferItems = append(bufferItems, pi)
		}
	}
	p.items = bufferItems

	go func(ctx context.Context) {

		// check if there is an idle connection from pool. if it is, then use it.
		for _, pi := range p.items {
			if pi.IsFree() {
				pi.Use()
				cpi <- pi
				return
			}
		}

		// no idle connections are found from the pool.
		// then perform another check.
		// if the total created connection is still lower than pool max conn size, create new one.
		p.RLock()
		shouldCreateNewPoolItem := len(p.items) < p.size
		p.RUnlock()

		if shouldCreateNewPoolItem {

			// creating new connection, end the routine if an error occurs
			pi, err := p.newItem()
			if err != nil {
				cerr <- err
				return
			}

			// add the newly created connection into pool
			p.Lock()
			p.items = append(p.items, pi)
			p.Unlock()

			// use newly created connection, then end the routine
			pi.retrieveDbPoolingInfo(p)
			pi.Use()
			cpi <- pi
			return
		}

		// block underneath will only be executed if the two criteria below are met:
		// 1. no idle connection is found from the pool
		// 2. cannot create new connection, because total created conns met allowed max conns

		// what will happen next, we'll wait until `p.Timeout`.
		// - if one connection is found idle and not closed before exceeding timeout, then use that one
		// - if timeout is exceeded, then return an error
		for done := false; !done; {
			select {
			case <-time.After(10 * time.Millisecond):
				for _, pi := range p.items {
					if pi.IsFree() && !pi.isClosed() {
						pi.retrieveDbPoolingInfo(p)
						pi.Use()
						cpi <- pi
					}
				}

			case <-ctx.Done():
				done = true
			}
		}
	}(ctx)

	select {
	case pi := <-cpi:
		//toolkit.Printfn("Connection is used. Size: %d Count: %d", p.Size(), p.Count())
		return pi, nil

	case err := <-cerr:
		return nil, toolkit.Errorf("unable to create new pool item. %s", err.Error())

	case <-ctx.Done():
		return nil, toolkit.Errorf("Pool size (%d) has been reached", p.size)
	}
}

// Count number of connection within connection pooling
func (p *DbPooling) Count() int {
	return len(p.items)
}

// FreeCount number of item has been released
func (p *DbPooling) FreeCount() int {
	i := 0
	for _, pi := range p.items {
		if pi.IsFree() && !pi.closed {
			i++
		}
	}
	return i
}

// ClosedCount number of item has been closed
func (p *DbPooling) ClosedCount() int {
	i := 0
	for _, pi := range p.items {
		if pi.closed {
			i++
		}
	}
	return i
}

// Size number of connection can be hold within the connection pooling
func (p *DbPooling) Size() int {
	return p.size
}

// Close all connection within connection pooling
func (p *DbPooling) Close() {
	p.Lock()
	for _, pi := range p.items {
		pi.conn.Close()
	}

	p.items = []*PoolItem{}
	p.Unlock()
}

func (p *DbPooling) newItem() (*PoolItem, error) {
	conn, err := p.fnNew()
	if err != nil {
		return nil, toolkit.Errorf("unable to open connection for DB pool. %s", err.Error())
	}

	pi := &PoolItem{conn: conn, used: false}
	pi.retrieveDbPoolingInfo(p)

	//-- auto release
	go func() {
		for {
			if pi == nil {
				return
			}

			if pi.AutoRelease == 0 {
				time.Sleep(100 * time.Millisecond)
			} else {
				select {
				case <-time.After(100 * time.Millisecond):
					diff := time.Now().Sub(pi.lastUsed)
					if diff > pi.AutoRelease && !pi.IsFree() {
						pi.Release()
					}
				}
			}
		}
	}()

	//-- auto close
	go func() {
		for {
			if pi == nil {
				return
			}

			if pi.AutoClose == 0 {
				time.Sleep(100 * time.Millisecond)
			} else {
				select {
				case <-time.After(100 * time.Millisecond):
					diff := time.Now().Sub(pi.lastUsed)
					if diff > pi.AutoClose && pi.IsFree() {
						pi.conn.Close()
						pi.Lock()
						pi.closed = true
						pi.Unlock()
						return
					}
				}
			}
		}
	}()

	return pi, nil
}

func (pi *PoolItem) retrieveDbPoolingInfo(p *DbPooling) {
	pi.AutoClose = p.AutoClose
	pi.AutoRelease = p.AutoRelease
}

func (pi *PoolItem) isClosed() bool {
	ret := false
	pi.RLock()
	ret = pi.closed
	pi.RUnlock()

	return ret
}

// Release PoolItem
func (pi *PoolItem) Release() {
	pi.Lock()
	pi.used = false
	pi.lastUsed = time.Now()
	pi.Unlock()
}

// IsFree check and return true if PoolItem is free
func (pi *PoolItem) IsFree() bool {
	free := false
	pi.RLock()
	free = !pi.used
	pi.RUnlock()

	return free
}

// Use mark that this PoolItem is used
func (pi *PoolItem) Use() {
	pi.Lock()
	pi.used = true
	pi.lastUsed = time.Now()
	pi.Unlock()
}

// Connection return PoolItem connection
func (pi *PoolItem) Connection() IConnection {
	return pi.conn
}
