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
	var e error
	if q.Parts == nil {
		return nil, errorlib.Error(packageName, modQuery,
			"Cursor", fmt.Sprintf("No Query Parts"))
	}

	aggregate := false
	dbname := q.Connection().Info().Database
	tablename := ""

	/*
		parts will return E - map{interface{}}interface{}
		where each interface{} returned is slice of interfaces --> []interface{}
	*/
	parts := crowd.From(q.Parts()).Group(func(x interface{}) interface{} {
		qp := x.(*dbox.QueryPart)
		return qp.PartType
	}, nil).Data

	fromParts, hasFrom := parts[dbox.QueryPartFrom]
	if hasFrom == false {
		return nil, errorlib.Error(packageName, "Query", "Cursor", "Invalid table name")
	}
	tablename = fromParts.([]interface{})[0].(*dbox.QueryPart).Value.(string)

	skip := 0
	if skipParts, hasSkip := parts[dbox.QueryPartSkip]; hasSkip {
		skip = skipParts.([]interface{})[0].(*dbox.QueryPart).
			Value.(int)
	}

	take := 0
	if takeParts, has := parts[dbox.QueryPartTake]; has {
		take = takeParts.([]interface{})[0].(*dbox.QueryPart).
			Value.(int)
	}

	var fields toolkit.M
	selectParts, hasSelect := parts[dbox.QueryPartSelect]
	if hasSelect {
		fields = toolkit.M{}
		for _, sl := range selectParts.([]interface{}) {
			qp := sl.(*dbox.QueryPart)
			for _, fid := range qp.Value.([]string) {
				fields.Set(fid, 1)
			}
		}
	} else {
		_, hasUpdate := parts[dbox.QueryPartUpdate]
		_, hasInsert := parts[dbox.QueryPartInsert]
		_, hasDelete := parts[dbox.QueryPartDelete]
		_, hasSave := parts[dbox.QueryPartSave]
		if hasUpdate || hasInsert || hasDelete || hasSave {
			return nil, errorlib.Error(packageName, modQuery, "Cursor",
				"Valid operation for a cursor is select only")
		}
	}
	//fmt.Printf("Result: %s \n", toolkit.JsonString(fields))
	//fmt.Printf("Database:%s table:%s \n", dbname, tablename)
	var sort []string
	sortParts, hasSort := parts[dbox.QueryPartSelect]
	if hasSort {
		sort = []string{}
		for _, sl := range sortParts.([]interface{}) {
			qp := sl.(*dbox.QueryPart)
			for _, fid := range qp.Value.([]string) {
				sort = append(sort, fid)
			}
		}
	}

	//where := toolkit.M{}
	var where interface{}
	whereParts, hasWhere := parts[dbox.QueryPartWhere]
	if hasWhere {
		fb := q.Connection().Fb()
		for _, p := range whereParts.([]interface{}) {
			fs := p.(*dbox.QueryPart).Value.([]*dbox.Filter)
			for _, f := range fs {
				fb.AddFilter(f)
			}
		}
		where, e = fb.Build()
		if e != nil {
			return nil, errorlib.Error(packageName, modQuery, "Cursor",
				e.Error())
		} else {
			//fmt.Printf("Where: %s", toolkit.JsonString(where))
		}
		//where = iwhere.(toolkit.M)
	}

	c := q.Connection()
	cursor := dbox.NewCursor(new(Cursor))
	cursor.SetConnection(q.Connection())
	if !aggregate {
		mgoColl := c.(*Connection).session.DB(dbname).C(tablename)
		mgoCursor := mgoColl.Find(where)
		count, e := mgoCursor.Count()
		if e != nil {
			//fmt.Println("Error: " + e.Error())
			return nil, errorlib.Error(packageName,
				modQuery, "Cursor", e.Error())
		}
		if fields != nil {
			mgoCursor = mgoCursor.Select(fields)
		}
		if hasSort {
			mgoCursor = mgoCursor.Sort(sort...)
		}
		if skip > 0 {
			mgoCursor = mgoCursor.Skip(skip)
		}
		if take > 0 {
			mgoCursor = mgoCursor.Limit(take)
		}
		cursor.(*Cursor).ResultType = QueryResultCursor
		cursor.(*Cursor).mgoCursor = mgoCursor
		cursor.(*Cursor).count = count
		//cursor.(*Cursor).mgoIter = mgoCursor.Iter()
	} else {
		pipes := toolkit.M{}
		mgoPipe := c.(*Connection).session.DB(dbname).C(tablename).
			Pipe(pipes).AllowDiskUse()
		//iter := mgoPipe.Iter()

		cursor.(*Cursor).ResultType = QueryResultPipe
		cursor.(*Cursor).mgoPipe = mgoPipe
		//cursor.(*Cursor).mgoIter = iter
	}
	return cursor, nil
}

func (q *Query) Exec(result interface{}, in toolkit.M) error {
	return nil
}
