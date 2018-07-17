package mongo

import (
	"errors"
	"fmt"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"gopkg.in/mgo.v2"
	"os"
	"regexp"
	"sync"
	"time"
)

const (
	packageName   = "eaciit.dbox.dbc.mongo"
	modConnection = "Connection"
)

type Connection struct {
	dbox.Connection

	session *mgo.Session
}

func init() {
	dbox.RegisterConnector("mongo", NewConnection)
}

func NewConnection(ci *dbox.ConnectionInfo) (dbox.IConnection, error) {
	c := new(Connection)

	if ci == nil {
		return c, errors.New("ConnectionInfo is not initialized")
	}

	if ci.Settings == nil {
		ci.Settings = toolkit.M{}
	}

	c.SetInfo(ci)
	c.SetFb(dbox.NewFilterBuilder(new(FilterBuilder)))

	return c, nil
}

var traceSession map[*mgo.Session][]byte
var traceLock sync.Mutex

func IsTraceEnable() bool {
	return traceSession != nil
}

func StartTrace() {
	traceLock.Lock()
	defer traceLock.Unlock()

	// already started?
	if traceSession != nil {
		return
	}

	traceSession = make(map[*mgo.Session][]byte)
	fmt.Fprintf(os.Stderr, "::Trace enable\n")
}

func PrintTrace() {
	traceLock.Lock()
	defer traceLock.Unlock()

	if traceSession == nil {
		fmt.Fprintf(os.Stderr, "::Trace is not enabled")
		return
	}

	fmt.Fprintf(os.Stderr, "::Connection Trace Result: %d probably leaked\n", len(traceSession))
	if len(traceSession) == 0 {
		return
	}

	count := 1
	for _, val := range traceSession {
		fmt.Fprintf(os.Stderr, "::Connection %d\n%s\n", count, string(val))
		count++
	}
}

func (c *Connection) Fb() dbox.IFilterBuilder {
	fb := new(FilterBuilder)
	fb.SetThis(fb)
	return fb
}

func (c *Connection) Connect() error {
	info := new(mgo.DialInfo)
	ci := c.Info()
	if ci == nil {
		return errorlib.Error(packageName, modConnection, "Connect", "ConnectionInfo is not initialized")
	}
	if ci.UserName != "" {
		info.Username = ci.UserName
		info.Password = ci.Password
		info.Source = "admin"
	}
	info.Addrs = []string{ci.Host}
	info.Database = ci.Database

	if ci.Settings == nil {
		ci.Settings = toolkit.M{}
	}

	poollimit := ci.Settings.GetInt("poollimit")
	if poollimit > 0 {
		info.PoolLimit = poollimit
	}

	timeout := ci.Settings.GetInt("timeout")
	if timeout > 0 {
		info.Timeout = time.Duration(timeout) * time.Second
	}

	//sess, e := mgo.Dial(info.Addrs[0])
	sess, e := mgo.DialWithInfo(info)
	if e != nil {
		return errorlib.Error(packageName, modConnection,
			"Connect", e.Error()+" "+ci.UserName+"@"+ci.Host+"/"+ci.Database)
	}
	sess.SetMode(mgo.Monotonic, true)
	c.session = sess
	return nil
}

func (c *Connection) NewQuery() dbox.IQuery {
	q := new(Query)
	q.SetConnection(c)
	q.SetThis(q)
	return q
}

func (c *Connection) ObjectNames(obj dbox.ObjTypeEnum) []string {
	mgoDb := c.session.DB(c.Info().Database)
	if obj == "" {
		obj = dbox.ObjTypeAll
	}

	astr := []string{}

	if obj == dbox.ObjTypeAll || obj == dbox.ObjTypeTable {
		cols, err := mgoDb.CollectionNames()
		if err != nil {
			return []string{}
		}

		for _, col := range cols {
			if cond, _ := regexp.MatchString("^(.*)((\\.(indexes)|\\.(js)))$", col); !cond {
				astr = append(astr, col)
			}
		}

	}

	if obj == dbox.ObjTypeAll || obj == dbox.ObjTypeProcedure {
		cols := mgoDb.C("system.js")
		res := []toolkit.M{}
		err := cols.Find(nil).All(&res)
		if err != nil {
			toolkit.Printf("%v\n", err.Error())
			return []string{}
		}

		// toolkit.Printf("%v\n", res)
		for _, col := range res {
			astr = append(astr, col["_id"].(string))
		}

	}

	return astr
}

func (c *Connection) Close() {
	if c.session != nil {
		c.session.Close()
	}
}
