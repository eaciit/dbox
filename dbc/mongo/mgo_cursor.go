package mongo

import (
	"github.com/eaciit/dbox"
	"gopkg.in/mgo.v2"
)

const (
	QueryResultCursor = "MongoCursor"
	QueryResultIter   = "MongoIter"
)

type Cursor struct {
	dbox.Cursor

	ResultType string
	mgoCursor  *mgo.Query
	mgoIter    *mgo.Iter

	count int
}

func (c *Cursor) Close() {

}

func (c *Cursor) Count() int {
	return c.count
}

func (c *Cursor) ResetFetch() error {
	return nil
}

func (c *Cursor) Fetch(out interface{}, n int, closeWhenDone bool) error {
	return nil
}
