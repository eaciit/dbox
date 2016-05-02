package odbc

import (
	"errors"
	"github.com/eaciit/dbox"
	err "github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"odbc"
)

const (
	modCursor = "Cursor"

	QueryResultCursor = "SQLCursor"
	QueryResultPipe   = "SQLPipe"
)

type Cursor struct {
	dbox.Cursor
	count, start int
	data         toolkit.Ms
	Sess         *odbc.Connection
}

func (c *Cursor) Count() int {
	return c.count
}

func (c *Cursor) ResetFetch() error {
	c.start = 0
	return nil
}

func (c *Cursor) Fetch(m interface{}, n int, closeWhenDone bool) error {
	end := c.start + n
	if end > c.count || n == 0 {
		end = c.count
	}

	if c.start >= c.count {
		return errors.New("No more data to fetched!")
	}
	e := toolkit.Serde(c.data[c.start:end], m, "json")
	if e != nil {
		return err.Error(packageName, modCursor, "Fetch", e.Error())
	}

	return nil
}

func (c *Cursor) Close() {
	c.Sess.Close()
}
