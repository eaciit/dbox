package mongo

import (
	_ "fmt"
	"github.com/eaciit/crowd"
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

	/*
		parts will return E - map{interface{}}interface{}
		where each interface{} returned is slice of interfaces --> []interface{}
	*/
	parts := crowd.From(q.Parts()).Group(func(x interface{}) interface{} {
		qp := x.(*dbox.QueryPart)
		return qp.PartType
	}, nil).Data

	selectParts, hasSelect := parts[dbox.QueryPartSelect]

	var fields toolkit.M
	if hasSelect {
		fields = toolkit.M{}
		for _, sl := range selectParts.([]interface{}) {
			qp := sl.(*dbox.QueryPart)
			for _, fid := range qp.Value.([]string) {
				fields.Set(fid, 1)
			}
		}
	}
	//fmt.Printf("Result: %s \n", toolkit.JsonString(fields))

	c := q.Connection()
	cursor := dbox.NewCursor(new(Cursor))
	cursor.SetConnection(q.Connection())
	if !aggregate {
		mgoCursor := c.(*Connection).session.DB(dbname).C(tablename).Find(f)
		if fields != nil {
			mgoCursor = mgoCursor.Select(fields)
		}
		cursor.(*Cursor).ResultType = QueryResultCursor
		cursor.(*Cursor).mgoCursor = mgoCursor
		cursor.(*Cursor).count, _ = mgoCursor.Count()
		//cursor.(*Cursor).mgoIter = mgoCursor.Iter()
	} else {
		pipes := toolkit.M{}
		mgoPipe := c.(*Connection).session.DB(dbname).C(tablename).
			Pipe(pipes).AllowDiskUse()
		iter := mgoPipe.Iter()

		cursor.(*Cursor).ResultType = QueryResultIter
		cursor.(*Cursor).mgoIter = iter
	}
	return cursor, nil
}

func (q *Query) Exec(result interface{}, in toolkit.M) error {
	return nil
}
