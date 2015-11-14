package mongo

import (
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
)

const (
	modQuery = "Query"
)

type Query struct {
	dbox.Query
}

func (q *Query) Cursor(in toolkit.M) (dbox.ICursor, error) {
	if q.Parts == nil {
		return nil, errorlib.Error(packageName, modQuery,
			"Cursor", "No Query Parts")
	}

	aggregate := false
	dbname := q.Connection().Info().Database
	tablename := ""
	f := toolkit.M{}

	c := q.Connection()

	cursor := dbox.NewCursor(new(Cursor))
	cursor.SetConnection(q.Connection)
	if !aggregate {
		mgoCursor := c.session.DB(dbname).C(tablename).Find(f)
		cursor.ResultType = MongoCusor
		cursor.mgoCursor = mgoCursor
	} else {
		pipes := toolkit.M{}
		mgoPipe := c.session.DB(dbname).C(tablename).Pipe(pipes)
		iter := mgoPipe.Iter()

		cursor.ResultType = MongoIter
		cursor.mgoIter = iter
	}
}

func (q *Query) Exec(result interface{}, in toolkit.M) error {
	return nil
}
