package hivetr

import (
	"errors"

	"github.com/kharism/dbox"
	"github.com/kharism/gohive"
)

type Cursor struct {
	rowSet   *gohive.RowSetR
	Conn     dbox.IConnection
	count    int
	rawQuery string
}

func NewCursor(conn dbox.IConnection, query *Query) *Cursor {
	c := &Cursor{}
	c.Conn = conn
	c.count = -1

	return c
}
func (c *Cursor) Close() {
	c.rowSet.Close()
}
func (c *Cursor) Count() int {
	return 0
}
func (c *Cursor) ResetFetch() error {
	return errors.New("NOT IMPLEMENTED")
}
func (c *Cursor) Fetch(target interface{}, num int, close bool) error {
	return errors.New("NOT IMPLEMENTED")
}

//--- getter
func (c *Cursor) Connection() dbox.IConnection {
	return nil
}

//-- setter
func (c *Cursor) SetConnection(conn dbox.IConnection) dbox.ICursor {
	c.Conn = conn
	return c
}
func (c *Cursor) SetThis(dbox.ICursor) dbox.ICursor {
	return c
}
