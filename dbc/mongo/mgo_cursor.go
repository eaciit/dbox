package mongo

import (
	"github.com/eaciit/dbox"
	"gopkg.in/mgo.v2"
)

type Cursor struct {
	dbox.Cursor

	ResultType string
	mgoCursor  *mgo.Cursor
	mgoIter    *mgoIter
}

func (c *Cursor) Close() {

}

func (c *Cursor) Count() int {
	return 0
}

func (c *Cursor) ResetFetch() error {
	return nil
}

func (c *Cursor) Fetch(out interface{}, n int, closeWhenDone true) error {
	return error
}
