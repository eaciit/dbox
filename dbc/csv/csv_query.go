package csv

import (
	"encoding/csv"
	"fmt"
	"github.com/eaciit/crowd"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"io"
	"os"
	"reflect"
)

const (
	modQuery = "Query"
)

type Query struct {
	dbox.Query

	file     *os.File
	tempFile *os.File
	reader   *csv.Reader
}

func (q *Query) File() *os.File {
	if q.file == nil {
		q.file = q.Connection().(*Connection).file
	}
	return q.file
}

func (q *Query) Reader() *csv.Reader {
	if q.reader == nil {
		q.reader = q.Connection().(*Connection).reader
	}
	return q.reader
}

func (q *Query) Close() {
	// if q.file != nil {
	// 	q.file.Close()
	// }
}

func (q *Query) Prepare() error {
	return nil
}

func (q *Query) Cursor(in toolkit.M) (dbox.ICursor, error) {
	var e error

	aggregate := false
	//	dbname := q.Connection().Info().Host
	//	tablename := ""

	parts := crowd.From(q.Parts()).Group(func(x interface{}) interface{} {
		qp := x.(*dbox.QueryPart)
		return qp.PartType
	}, nil).Data

	/*	fromParts, hasFrom := parts[dbox.QueryPartFrom]
		if hasFrom == false {
			return nil, errorlib.Error(packageName, "Query", "Cursor", "Invalid table name")
		}
		tablename = fromParts.([]interface{})[0].(*dbox.QueryPart).Value.(string)*/
	/*
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
	*/
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

	cursor := dbox.NewCursor(new(Cursor))
	cursor = cursor.SetConnection(q.Connection())

	cursor.(*Cursor).file = q.File()
	cursor.(*Cursor).reader = q.Reader()
	cursor.(*Cursor).headerColumn = q.Connection().(*Connection).headerColumn

	if e != nil {
		return nil, errorlib.Error(packageName, modQuery, "Cursor", e.Error())
	}

	if !aggregate {
		cursor.(*Cursor).ConditionVal.Find = where

		if fields != nil {
			cursor.(*Cursor).ConditionVal.Select = fields
		}

		if hasSort {
			cursor.(*Cursor).ConditionVal.Sort = sort
		}

		// if fields != nil {
		// 	mgoCursor = mgoCursor.Select(fields)
		// }
		// if hasSort {
		// 	mgoCursor = mgoCursor.Sort(sort...)
		// }
		// if skip > 0 {
		// 	mgoCursor = mgoCursor.Skip(skip)
		// }
		// if take > 0 {
		// 	mgoCursor = mgoCursor.Limit(take)
		// }
		// cursor.(*Cursor).ResultType = QueryResultCursor
		// cursor.(*Cursor).mgoCursor = mgoCursor
		// cursor.(*Cursor).count = count

		//cursor.(*Cursor).mgoIter = mgoCursor.Iter()
	} else {
		/*		pipes := toolkit.M{}
				mgoPipe := session.DB(dbname).C(tablename).
					Pipe(pipes).AllowDiskUse()
				//iter := mgoPipe.Iter()

				cursor.(*Cursor).ResultType = QueryResultPipe
				cursor.(*Cursor).mgoPipe = mgoPipe
				//cursor.(*Cursor).mgoIter = iter
		*/
	}
	return cursor, nil
}

func (q *Query) Exec(parm toolkit.M) error {
	var e error

	if parm == nil {
		parm = toolkit.M{}
	}

	// dbname := q.Connection().Info().Database
	// tablename := ""

	if parm == nil {
		parm = toolkit.M{}
	}
	data := parm.Get("data", nil)

	parts := crowd.From(q.Parts()).Group(func(x interface{}) interface{} {
		qp := x.(*dbox.QueryPart)
		return qp.PartType
	}, nil).Data

	// fromParts, hasFrom := parts[dbox.QueryPartFrom]
	// if !hasFrom {
	// 	return errorlib.Error(packageName, "Query", modQuery, "Invalid table name")
	// }
	// tablename = fromParts.([]interface{})[0].(*dbox.QueryPart).Value.(string)

	// var where interface{}
	commandType := ""
	//	multi := false

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

	// if data == nil {
	// 	multi = true
	// } else {
	// 	if where == nil {
	// 		id := toolkit.Id(data)
	// 		if id != nil {
	// 			where = (toolkit.M{}).Set("_id", id)
	// 		}
	// 	} else {
	// 		multi = true
	// 	}
	// }

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
			return errorlib.Error(packageName, modQuery, "Cursor", e.Error())
		}
	}

	q.Connection().(*Connection).TypeOpenFile = TypeOpenFile_Append
	if hasDelete || hasUpdate {
		q.Connection().(*Connection).TypeOpenFile = TypeOpenFile_Create
	}

	q.Connection().(*Connection).ExecOpr = false
	e = q.Connection().(*Connection).StartSessionWrite()

	if e != nil {
		return errorlib.Error(packageName, "Query", modQuery, e.Error())
	}

	writer := q.Connection().(*Connection).writer
	reader := q.Connection().(*Connection).reader

	// multiExec := q.Config("multiexec", false).(bool)
	// if !multiExec && !q.usePooling && session != nil {
	// 	defer Connection().(*Connection).Close()
	// }

	switch commandType {
	case dbox.QueryPartInsert:
		var dataTemp []string
		val := reflect.ValueOf(data)
		for _, v := range q.Connection().(*Connection).headerColumn {
			for i := 0; i < val.NumField(); i++ {
				if v == val.Type().Field(i).Name {
					dataTemp = append(dataTemp, fmt.Sprintf("%s", val.Field(i)))
					break
				}
			}
		}

		writer.Write(dataTemp)
		writer.Flush()
	case dbox.QueryPartDelete:
		condFind := make(map[int]WhereCond)
		for _, key := range reflect.ValueOf(where).MapKeys() {
			temp := reflect.ValueOf(reflect.ValueOf(where).MapIndex(key).Interface())
			// fmt.Println(temp.String())
			for n, val := range q.Connection().(*Connection).headerColumn {
				if val == key.String() {
					condFind[n] = WhereCond{"EQ", temp.String()}
				}
			}
			break
		}

		writer.Write(q.Connection().(*Connection).headerColumn)
		writer.Flush()

		for {
			isAppend := true

			dataTemp, e := reader.Read()
			for i, val := range dataTemp {
				condVal, found := condFind[i]
				if found {
					if condVal.operator == "EQ" {
						if condVal.condition == val {
							isAppend = false
						}
					}
				}
			}

			if e == io.EOF {
				if isAppend && dataTemp != nil {
					writer.Write(dataTemp)
					writer.Flush()
				}
				break
			} else if e != nil {
				return errorlib.Error(packageName, modQuery, "Delete", e.Error())
			}
			if isAppend && dataTemp != nil {
				writer.Write(dataTemp)
				writer.Flush()
			}
		}
	case dbox.QueryPartUpdate:
		if data == nil {
			break
		}

		// valChange := make(map[int]string)
		condFind := make(map[int]WhereCond)

		valChange := reflect.ValueOf(data)

		// for i := 0; i < val.NumField(); i++ {
		// 	for n, v := range q.Connection().(*Connection).headerColumn {
		// 		if v == val.Type().Field(i).Name {
		// 			valChange[n] = fmt.Sprintf("%s", val.Field(i))
		// 			break
		// 		}
		// 	}
		// }

		for _, key := range reflect.ValueOf(where).MapKeys() {
			temp := reflect.ValueOf(reflect.ValueOf(where).MapIndex(key).Interface())
			for n, val := range q.Connection().(*Connection).headerColumn {
				if val == key.String() {
					condFind[n] = WhereCond{"EQ", temp.String()}
				}
			}
			break
		}

		writer.Write(q.Connection().(*Connection).headerColumn)
		writer.Flush()

		for {
			foundChange := false

			dataTemp, e := reader.Read()
			for i, val := range dataTemp {
				condVal, found := condFind[i]
				if found {
					if condVal.operator == "EQ" {
						if condVal.condition == val {
							foundChange = true
						}
					}
				}
			}

			if foundChange {
				for i := 0; i < valChange.NumField(); i++ {
					for n, v := range q.Connection().(*Connection).headerColumn {
						if v == valChange.Type().Field(i).Name && valChange.Field(i).String() != "" {
							dataTemp[n] = fmt.Sprintf("%s", valChange.Field(i))
							break
						}
					}
				}
				//				fmt.Println(dataTemp)
			}

			if e == io.EOF {
				if dataTemp != nil {
					writer.Write(dataTemp)
					writer.Flush()
				}
				break
			} else if e != nil {
				return errorlib.Error(packageName, modQuery, "Delete", e.Error())
			}
			if dataTemp != nil {
				writer.Write(dataTemp)
				writer.Flush()
			}
		}
	}

	q.Connection().(*Connection).ExecOpr = true
	e = q.Connection().(*Connection).EndSessionWrite()

	// if commandType == dbox.QueryPartInsert {
	// 	e = mgoColl.Insert(data)
	// } else if commandType == dbox.QueryPartUpdate {
	// 	if multi {
	// 		_, e = mgoColl.UpdateAll(where, data)
	// 	} else {
	// 		e = mgoColl.Update(where, data)
	// 		if e != nil {
	// 			e = fmt.Errorf("%s [%v]", e.Error(), where)
	// 		}
	// 	}
	// } else if commandType == dbox.QueryPartDelete {
	// 	if multi {
	// 		_, e = mgoColl.RemoveAll(where)
	// 	} else {
	// 		e = mgoColl.Remove(where)
	// 		if e != nil {
	// 			e = fmt.Errorf("%s [%v]", e.Error(), where)
	// 		}
	// 	}
	// } else if commandType == dbox.QueryPartSave {
	// 	_, e = mgoColl.Upsert(where, data)
	// }
	// if e != nil {
	// 	return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
	// }

	return nil
}
