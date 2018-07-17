package hivetr

import (
	"errors"
	"fmt"

	"github.com/eaciit/toolkit"

	"github.com/kharism/dbox"
	"github.com/kharism/gohive"
)

type Cursor struct {
	rowSet   *gohive.RowSetR
	Conn     dbox.IConnection
	count    int64
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
	if num > 1 && !toolkit.IsSlice(target) {
		return errors.New("target must be slice")
	}
	if c.rowSet == nil {
		return errors.New("Rowset is nil")
	}
	if num == 1 && !toolkit.IsSlice(target) {
		if c.rowSet.Next() {
			c.rowSet.ScanObject(target)
		} else {
			return errors.New("No more data")
		}
	} else {
		noLimit := false
		if num == 0 {
			noLimit = true
		}
		outputTKM := []toolkit.M{}
		counter := 0
		for {
			newTarget := toolkit.M{}
			if c.rowSet.Next() {
				err := c.rowSet.ScanObject(&newTarget)
				if err != nil {
					fmt.Println("FETCH ERROR", err.Error)
				}
				counter++
				outputTKM = append(outputTKM, newTarget)
			} else {
				toolkit.Serde(outputTKM, target, "json")
				return nil
			}
			if !noLimit {
				if counter == num {
					toolkit.Serde(outputTKM, target, "json")
				}
			}
		}
	}
	return nil
}

//--- getter
func (c *Cursor) Connection() dbox.IConnection {
	return c.Conn
}

//-- setter
func (c *Cursor) SetConnection(conn dbox.IConnection) dbox.ICursor {
	c.Conn = conn
	return c
}
func (c *Cursor) SetThis(dbox.ICursor) dbox.ICursor {
	return c
}
