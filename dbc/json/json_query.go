package json

import (
	"encoding/json"
	"fmt"
	"github.com/eaciit/crowd"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"io/ioutil"
	"os"
	"strings"
)

const (
	modQuery = "Query"
)

type Query struct {
	dbox.Query

	session *os.File
}

func (q *Query) Session() *os.File {
	if q.session == nil {
		q.session = q.Connection().(*Connection).session
	}
	return q.session
}

func (q *Query) Close() {
	q.session.Close()
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

	session := q.Session()

	t, _ := ioutil.ReadFile(session.Name())
	data := toolkit.M{}
	dec := json.NewDecoder(strings.NewReader(string(t)))
	dec.Decode(&data)

	cursor := dbox.NewCursor(new(Cursor))
	cursor.(*Cursor).session = session
	cursor.(*Cursor).tempFile = t

	/*
		parts will return E - map{interface{}}interface{}
		where each interface{} returned is slice of interfaces --> []interface{}
	*/
	parts := crowd.From(q.Parts()).Group(func(x interface{}) interface{} {
		qp := x.(*dbox.QueryPart)
		return qp.PartType
	}, nil).Data

	var fields []string
	selectParts, hasSelect := parts[dbox.QueryPartSelect]
	if hasSelect {
		// fields = toolkit.M{}
		for _, sl := range selectParts.([]interface{}) {
			qp := sl.(*dbox.QueryPart)
			for _, fid := range qp.Value.([]string) {
				fields = append(fields, fid)
				// fields.Set(fid, fid)
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

	if !aggregate {
		var jsonCursor interface{}
		var dataInterface interface{}
		json.Unmarshal(t, &dataInterface)
		// fmt.Printf("dataInterface: %v \n", dataInterface)
		count, ok := dataInterface.([]interface{})
		if !ok {
			//fmt.Println("Error: " + e.Error())
			return nil, errorlib.Error(packageName,
				modQuery, "Cursor", e.Error())
		}
		cursor.(*Cursor).count = len(count)
		// fmt.Printf("fields:%v\n", fields)
		if fields != nil {
			jsonCursor = fields
		}

		cursor.(*Cursor).ResultType = QueryResultCursor
		cursor.(*Cursor).jsonCursor = jsonCursor
	} else {
		cursor.(*Cursor).ResultType = QueryResultPipe
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

	// dbname := q.Connection().Info().Database
	tablename := ""

	if parm == nil {
		parm = toolkit.M{}
	}
	data := parm.Get("data", nil)

	/*
		p arts will return E - map{interface{}}interface{}
		where each interface{} returned is slice of interfaces --> []interface{}
	*/
	parts := crowd.From(q.Parts()).Group(func(x interface{}) interface{} {
		qp := x.(*dbox.QueryPart)
		/*
			fmt.Printf("[%s] QP = %s \n",
				toolkit.Id(data),
				toolkit.JsonString(qp))
		*/
		return qp.PartType
	}, nil).Data

	fromParts, hasFrom := parts[dbox.QueryPartFrom]
	if !hasFrom {
		/*
			fmt.Printf("Data:\n%s\nParts:\n%s\nGrouped:\n%s\n",
				toolkit.JsonString(data),
				toolkit.JsonString(q.Parts()),
				toolkit.JsonString(parts))
		*/
		return errorlib.Error(packageName, "Query", modQuery, "Invalid table name")
	}
	tablename = fromParts.([]interface{})[0].(*dbox.QueryPart).Value.(string)
	fmt.Printf("table:%s \n", tablename)
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

	if data == nil {
		//---
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

	if multi {
		// 		_, e = mgoColl.UpdateAll(where, data)
	}

	if e != nil {
		return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
	}
	return nil
}
