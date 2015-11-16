package mongo

import (
	"fmt"
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
			"Cursor", fmt.Sprintf("No Query Parts"))
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

	fromParts, hasFrom := parts[dbox.QueryPartFrom]
	selectParts, hasSelect := parts[dbox.QueryPartSelect]

	if hasFrom == false {
		return nil, errorlib.Error(packageName, "Query", "Cursor", "Invalid table name")
	}
	tablename = fromParts.([]interface{})[0].(*dbox.QueryPart).Value.(string)

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
	//fmt.Printf("Database:%s table:%s \n", dbname, tablename)

	c := q.Connection()
	cursor := dbox.NewCursor(new(Cursor))
	cursor.SetConnection(q.Connection())
	if !aggregate {
		mgoColl := c.(*Connection).session.DB(dbname).C(tablename)
		mgoCursor := mgoColl.Find(f)
		count, e := mgoCursor.Count()
		if e != nil {
			//fmt.Println("Error: " + e.Error())
			return nil, errorlib.Error(packageName,
				modQuery, "Cursor", e.Error())
		}
		if fields != nil {
			mgoCursor = mgoCursor.Select(fields)
		}
		cursor.(*Cursor).ResultType = QueryResultCursor
		cursor.(*Cursor).mgoCursor = mgoCursor
		cursor.(*Cursor).count = count
		cursor.(*Cursor).mgoIter = mgoCursor.Iter()
	} else {
		pipes := toolkit.M{}
		mgoPipe := c.(*Connection).session.DB(dbname).C(tablename).
			Pipe(pipes).AllowDiskUse()
		iter := mgoPipe.Iter()

		cursor.(*Cursor).ResultType = QueryResultPipe
		cursor.(*Cursor).mgoPipe = mgoPipe
		cursor.(*Cursor).mgoIter = iter
	}
	return cursor, nil
}

func (q *Query) Exec(result interface{}, in toolkit.M) error {
	return nil
}
