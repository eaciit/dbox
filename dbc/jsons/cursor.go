package jsons

import (
	"github.com/eaciit/dbox"
)

type Cursor struct {
	dbox.Cursor
	query *Query
}

func (c *Cursor) Close() {

}

func (c *Cursor) Count() int {
	return 0
}

func (c *Cursor) ResetFetch() error {
	return nil
}

func (c *Cursor) Fetch(m interface{}, n int, closeWhenDone bool) error {
	return nil
}

func newCursor(q *Query) *Cursor {
	c := new(Cursor)
	c.query = q
	return c
}
