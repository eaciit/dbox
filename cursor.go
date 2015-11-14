package dbox

import (
	"github.com/eaciit/errorlib"
)

const (
	modCursor = "Cursor"
)

type ICursor interface {
	Close()
	Count() int
	ResetFetch() error
	Fetch(interface{}, int, bool) error

	//--- getter
	Connection() IConnection

	//-- setter
	SetConnection(IConnection) ICursor
	SetThis(ICursor) ICursor
}

type Cursor struct {
	connection IConnection
	thisO      ICursor
}

func NewCursor(c ICursor) ICursor {
	c.SetThis(c)
	return c
}

func (c *Cursor) this() ICursor {
	if c.thisO == nil {
		return c
	} else {
		return c.thisO
	}
}

func (c *Cursor) SetThis(cursor ICursor) ICursor {
	c.thisO = cursor
	return cursor
}

func (c *Cursor) Connection() IConnection {
	return c.connection
}

func (c *Cursor) SetConnection(conn IConnection) ICursor {
	c.connection = conn
	return c.this()
}

func (c *Cursor) Close() {
}

func (c *Cursor) Count() int {
	return 0
}

func (c *Cursor) ResetFetch() error {
	return errorlib.Error(packageName, modCursor, "ResetFetch", errorlib.NotYetImplemented)
}

func (c *Cursor) Fetch(o interface{}, n int,
	closeWhenDone bool) error {
	return errorlib.Error(packageName, modCursor, "Fetch", errorlib.NotYetImplemented)
}
