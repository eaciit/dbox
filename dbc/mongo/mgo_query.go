package mongo

import (
	"fmt"
	"github.com/eaciit/crowd"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"gopkg.in/mgo.v2"
	"strings"
)

const (
	modQuery = "Query"
)

type Query struct {
	dbox.Query

	session    *mgo.Session
	usePooling bool
}

func (q *Query) Session() *mgo.Session {
	q.usePooling = q.Config("pooling", false).(bool)
	if q.session == nil {
		if q.usePooling {
			q.session = q.Connection().(*Connection).session
		} else {
			q.session = q.Connection().(*Connection).session.Clone()
		}
	}
	return q.session
}

func (q *Query) Close() {
	if q.session != nil && q.usePooling == false {
		q.session.Close()
	}
}

func (q *Query) Prepare() error {
	return nil
}

func (q *Query) Cursor(in toolkit.M) (dbox.ICursor, error) {
	var e error
	/*
		if q.Parts == nil {
			return nil, errorlib.Error(packageName, modQuery,
				"Cursor", fmt.Sprintf("No Query Parts"))
		}
	*/

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

	//return nil, errorlib.Error(packageName, modQuery, "Cursor", "asdaa")
	//fmt.Printf("Query parts: %s\n", toolkit.JsonString(q.Parts()))
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

	aggrParts, hasAggr := parts[dbox.QueryPartAggr]
	aggrExpression := toolkit.M{}
	if hasAggr {
		aggregate = true
		aggrElements := func() []*dbox.QueryPart {
			var qps []*dbox.QueryPart
			for _, v := range aggrParts.([]interface{}) {
				qps = append(qps, v.(*dbox.QueryPart))
			}
			return qps
		}()
		for _, el := range aggrElements {
			aggr := el.Value.(dbox.AggrInfo)
			//if aggr.Op == dbox.AggrSum {
			aggrExpression.Set(aggr.Alias, toolkit.M{}.Set(aggr.Op, aggr.Field))
			//}
		}
		//toolkit.Printf("Aggr: %s\n", toolkit.JsonString(aggrExpression))
	}
	partGroup, hasGroup := parts[dbox.QueryPartGroup]
	if hasGroup {
		aggregate = true
		groups := func() toolkit.M {
			s := toolkit.M{}
			for _, v := range partGroup.([]interface{}) {
				gs := v.(*dbox.QueryPart).Value.([]string)
				for _, g := range gs {
					if strings.TrimSpace(g) != "" {
						s.Set(g, "$"+g)
					}
				}
			}
			return s
		}()
		if len(groups) == 0 {
			aggrExpression.Set("_id", "")
		} else {
			aggrExpression.Set("_id", groups)
		}
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
	sortParts, hasSort := parts[dbox.QueryPartOrder]
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

	pipes := []toolkit.M{}
	pipe := parts["pipe"]
	if pipe != nil {
		aggregate = true
		pipes = pipe.([]interface{})[0].(*dbox.QueryPart).Value.([]toolkit.M)
	}

	session := q.Session()
	mgoColl := session.DB(dbname).C(tablename)
	cursor := dbox.NewCursor(new(Cursor))
	cursor.(*Cursor).session = session
	cursor.(*Cursor).isPoolingSession = q.usePooling

	if aggregate == true {
		if len(pipes) == 0 {
			pipes = append(pipes, toolkit.M{}.Set("$group", aggrExpression))
		}
		if hasWhere {
			pipes = append(append([]toolkit.M{}, toolkit.M{}.Set("$match", where)), pipes...)
		}
		mgoPipe := session.DB(dbname).C(tablename).
			Pipe(pipes).AllowDiskUse()
		//toolkit.Printf("Pipe: %s \n", toolkit.JsonString(pipes))
		//iter := mgoPipe.Iter()

		cursor.(*Cursor).ResultType = QueryResultPipe
		cursor.(*Cursor).mgoPipe = mgoPipe
		//cursor.(*Cursor).mgoIter = iter

	} else {
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
	}

	if cursor == nil {
		return nil, errorlib.Error(packageName, modQuery, "Cursor", "Unable to initialize cursor. This is likely caused by unimplemented command or invalid series of query")
	}
	return cursor, nil
}

func (q *Query) Exec(parm toolkit.M) error {
	var e error
	if parm == nil {
		parm = toolkit.M{}
	}
	/*
		if q.Parts == nil {
			return errorlib.Error(packageName, modQuery,
				"Cursor", fmt.Sprintf("No Query Parts"))
		}
	*/

	dbname := q.Connection().Info().Database
	tablename := ""

	if parm == nil {
		parm = toolkit.M{}
	}
	data := parm.Get("data", nil)

	/*
		parts will return E - map{interface{}}interface{}
		where each interface{} returned is slice of interfaces --> []interface{}
	*/
	parts := crowd.From(q.Parts()).Group(func(x interface{}) interface{} {
		qp := x.(*dbox.QueryPart)
		return qp.PartType
	}, nil).Data

	fromParts, hasFrom := parts[dbox.QueryPartFrom]
	if !hasFrom {
		return errorlib.Error(packageName, modQuery, "Exec", "Invalid table name")
	}
	tablename = fromParts.([]interface{})[0].(*dbox.QueryPart).Value.(string)

	var where interface{}
	commandType := ""
	multi := false

	_, hasDelete := parts[dbox.QueryPartDelete]
	_, hasInsert := parts[dbox.QueryPartInsert]
	_, hasUpdate := parts[dbox.QueryPartUpdate]
	_, hasSave := parts[dbox.QueryPartSave]

	if hasDelete {
		commandType = dbox.QueryPartDelete
	} else if hasInsert {
		commandType = dbox.QueryPartInsert
	} else if hasUpdate {
		commandType = dbox.QueryPartUpdate
	} else if hasSave {
		commandType = dbox.QueryPartSave
	}

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
			return errorlib.Error(packageName, modQuery, "Exec",
				e.Error())
		} else {
			//fmt.Printf("Where: %s\n", toolkit.JsonString(where))
		}
	}

	if data == nil {
		multi = true
	} else {
		if where == nil {
			id := toolkit.Id(data)
			if id != nil {
				where = (toolkit.M{}).Set("_id", id)
			}
		} else {
			multi = true
		}
	}

	session := q.Session()

	multiExec := q.Config("multiexec", false).(bool)
	if !multiExec && !q.usePooling && session != nil {
		defer session.Close()
	}
	mgoColl := session.DB(dbname).C(tablename)
	if commandType == dbox.QueryPartInsert {
		e = mgoColl.Insert(data)
	} else if commandType == dbox.QueryPartUpdate {
		if multi {
			dataM, e := toolkit.ToM(data)
			if e != nil {
				return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
			}
			if len(dataM) == 0 {
				return errorlib.Error(packageName, modQuery+".Exec", commandType, "Update data points is empty")
			}
			updatedData := toolkit.M{}.Set("$set", dataM)
			_, e = mgoColl.UpdateAll(where, updatedData)
		} else {
			e = mgoColl.Update(where, data)
			if e != nil {
				e = fmt.Errorf("%s [%v]", e.Error(), where)
			}
		}
	} else if commandType == dbox.QueryPartDelete {
		if multi {
			_, e = mgoColl.RemoveAll(where)
		} else {
			e = mgoColl.Remove(where)
			if e != nil {
				e = fmt.Errorf("%s [%v]", e.Error(), where)
			}
		}
	} else if commandType == dbox.QueryPartSave {
		_, e = mgoColl.Upsert(where, data)
	}
	if e != nil {
		return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
	}
	return nil
}
