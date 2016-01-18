package jsons

import (
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
)

type Cursor struct {
	dbox.Cursor
	indexes []int
	where   []*dbox.Filter

	q            *Query
	currentIndex int
}

func (c *Cursor) Close() {
}

func (c *Cursor) Count() int {
	var count int
	if c.where == nil {
		count = len(c.q.data)

	} else {
		count = len(c.indexes)
	}
	return count
}

func (c *Cursor) ResetFetch() error {
	if c.where != nil {
		c.indexes = dbox.Find(c.q.data, c.where)
	}
	c.currentIndex = 0
	return nil
}

func (c *Cursor) Fetch(m interface{}, n int, closeWhenDone bool) error {
	var source []toolkit.M
	var lower, upper int

	lower = c.currentIndex
	upper = lower + n

	if n == 0 {
		if c.where == nil {
			upper = len(c.q.data)
		} else {
			upper = len(c.indexes)
		}
	} else if n == 1 {
		upper = lower
	} else {
		upper = lower + n
		if c.where == nil {
			if upper > len(c.q.data) {
				upper = len(c.q.data)
			}
		} else {
			if upper > len(c.indexes) {
				upper = len(c.indexes)
			}
		}
	}

	if c.where == nil {
		source = c.q.data[lower:upper]
	} else {
		for _, v := range c.indexes[lower:upper] {
			if v < len(c.q.data) {
				source = append(c.q.data, c.q.data[v])

			}
		}
	}
	e := toolkit.Serde(&source, &m, "json")
	if e != nil {
		return errorlib.Error(packageName, modCursor, "Fetch", e.Error())
	}
	return nil
}

func newCursor(q *Query) *Cursor {
	c := new(Cursor)
	c.q = q
	return c
}
