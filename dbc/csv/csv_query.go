package csv

import (
	"encoding/csv"
	// "fmt"
	"github.com/eaciit/cast"
	"github.com/eaciit/crowd"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"io"
	"os"
	"reflect"
	// "regexp"
	"strings"
	// "time"
)

const (
	modQuery = "Query"
)

type Query struct {
	dbox.Query

	file        *os.File
	tempFile    *os.File
	reader      *csv.Reader
	save        bool
	updatessave bool
}

type QueryCondition struct {
	Select  toolkit.M
	indexes []int
	where   []*dbox.Filter
	Sort    []string
	skip    int
	limit   int
}

func (w *QueryCondition) getCondition(dataCheck toolkit.M) bool {
	resBool := true

	if len(w.where) > 0 {
		resBool = dbox.MatchM(dataCheck, w.where)
	}

	return resBool
}

// func foundCondition(dataCheck toolkit.M, cond toolkit.M) bool {
// 	resBool := true

// 	for key, val := range cond {
// 		if key == "$and" || key == "$or" {
// 			for i, sVal := range val.([]interface{}) {
// 				rVal := sVal.(map[string]interface{})
// 				mVal := toolkit.M{}
// 				for rKey, mapVal := range rVal {
// 					mVal.Set(rKey, mapVal)
// 				}

// 				xResBool := foundCondition(dataCheck, mVal)
// 				if key == "$and" {
// 					resBool = resBool && xResBool
// 				} else {
// 					if i == 0 {
// 						resBool = xResBool
// 					} else {
// 						resBool = resBool || xResBool
// 					}
// 				}
// 			}
// 		} else {
// 			if reflect.ValueOf(val).Kind() == reflect.Map {
// 				mVal := val.(map[string]interface{})
// 				tomVal, _ := toolkit.ToM(mVal)
// 				switch {
// 				case tomVal.Has("$ne"):
// 					if tomVal["$ne"].(string) == dataCheck.Get(key, "").(string) {
// 						resBool = false
// 					}
// 				case tomVal.Has("$regex"):
// 					resBool, _ = regexp.MatchString(tomVal["$regex"].(string), dataCheck.Get(key, "").(string))
// 				case tomVal.Has("$gt"):
// 					if tomVal["$gt"].(string) >= dataCheck.Get(key, "").(string) {
// 						resBool = false
// 					}
// 				case tomVal.Has("$gte"):
// 					if tomVal["$gte"].(string) > dataCheck.Get(key, "").(string) {
// 						resBool = false
// 					}
// 				case tomVal.Has("$lt"):
// 					if tomVal["$lt"].(string) <= dataCheck.Get(key, "").(string) {
// 						resBool = false
// 					}
// 				case tomVal.Has("$lte"):
// 					if tomVal["$lte"].(string) < dataCheck.Get(key, "").(string) {
// 						resBool = false
// 					}
// 				}
// 			} else if reflect.ValueOf(val).Kind() == reflect.String && val != dataCheck.Get(key, "").(string) {
// 				resBool = false
// 			}
// 		}
// 	}

// 	return resBool
// }

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
	if q.save {
		_ = q.Connection().(*Connection).EndSessionWrite()
	}
}

func (q *Query) Prepare() error {
	return nil
}

func (q *Query) Cursor(in toolkit.M) (dbox.ICursor, error) {
	var e error

	aggregate := false

	if q.Connection().(*Connection).setNewHeader {
		q.Connection().(*Connection).Close()
		filename := q.Connection().(*Connection).Info().Host
		os.Remove(filename)
		return nil, errorlib.Error(packageName, "Cursor", modQuery, "Only Insert Query Permited")
	}

	quyerParts := q.Parts()
	c := crowd.From(&quyerParts)

	groupParts := c.Group(func(x interface{}) interface{} {
		return x.(*dbox.QueryPart).PartType
	}, nil).Exec()

	parts := map[interface{}]interface{}{}
	if len(groupParts.Result.Data().([]crowd.KV)) > 0 {
		for _, kv := range groupParts.Result.Data().([]crowd.KV) {
			parts[kv.Key] = kv.Value
		}
	}

	skip := 0
	if skipParts, hasSkip := parts[dbox.QueryPartSkip]; hasSkip {
		skip = skipParts.([]*dbox.QueryPart)[0].
			Value.(int)
	}

	take := 0
	if takeParts, has := parts[dbox.QueryPartTake]; has {
		take = takeParts.([]*dbox.QueryPart)[0].
			Value.(int)
	}

	var fields toolkit.M
	selectParts, hasSelect := parts[dbox.QueryPartSelect]
	if hasSelect {
		fields = toolkit.M{}
		for _, sl := range selectParts.([]*dbox.QueryPart) {
			// qp := sl.(*dbox.QueryPart)
			for _, fid := range sl.Value.([]string) {
				fields.Set(strings.ToLower(fid), 1)
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
		for _, sl := range sortParts.([]*dbox.QueryPart) {
			// qp := sl.(*dbox.QueryPart)
			for _, fid := range sl.Value.([]string) {
				sort = append(sort, fid)
			}
		}
	}

	var where []*dbox.Filter
	whereParts, hasWhere := parts[dbox.QueryPartWhere]
	if hasWhere {
		for _, p := range whereParts.([]*dbox.QueryPart) {
			fs := p.Value.([]*dbox.Filter)
			for _, f := range fs {
				// if len(in) > 0 {
				f = ReadVariable(f, in)
				// }
				where = append(where, f)
			}
		}
	}

	cursor := dbox.NewCursor(new(Cursor))
	cursor = cursor.SetConnection(q.Connection())

	cursor.(*Cursor).file = q.File()
	cursor.(*Cursor).reader = q.Reader()
	cursor.(*Cursor).headerColumn = q.Connection().(*Connection).headerColumn
	cursor.(*Cursor).count = 0
	if e != nil {
		return nil, errorlib.Error(packageName, modQuery, "Cursor", e.Error())
	}

	if !aggregate {
		cursor.(*Cursor).ConditionVal.where = where
		if fields != nil {
			cursor.(*Cursor).ConditionVal.Select = fields
		}

		if hasSort {
			cursor.(*Cursor).ConditionVal.Sort = sort
		}
		cursor.(*Cursor).ConditionVal.skip = skip
		cursor.(*Cursor).ConditionVal.limit = take
		if skip > 0 && take > 0 {
			cursor.(*Cursor).ConditionVal.limit += skip
		}

		e = cursor.(*Cursor).generateIndexes()

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
	if e != nil {
		return nil, errorlib.Error(packageName, modQuery, "Cursor", e.Error())
	}
	return cursor, nil
}

func (q *Query) Exec(parm toolkit.M) error {
	var e error
	q.save = false

	// useHeader := q.Connection().Info().Settings.Get("useheader", false).(bool)
	if parm == nil {
		parm = toolkit.M{}
	}

	data := toolkit.M{}
	if parm.Has("data") {
		data, _ = toolkit.ToM(parm["data"])
	}

	quyerParts := q.Parts()
	c := crowd.From(&quyerParts)

	groupParts := c.Group(func(x interface{}) interface{} {
		return x.(*dbox.QueryPart).PartType
	}, nil).Exec()

	parts := map[interface{}]interface{}{}
	if len(groupParts.Result.Data().([]crowd.KV)) > 0 {
		for _, kv := range groupParts.Result.Data().([]crowd.KV) {
			parts[kv.Key] = kv.Value
		}
	}

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
		q.save = true
	}

	var where []*dbox.Filter
	whereParts, hasWhere := parts[dbox.QueryPartWhere]
	if hasWhere {
		for _, p := range whereParts.([]*dbox.QueryPart) {
			fs := p.Value.([]*dbox.Filter)
			for _, f := range fs {
				// if len(parm) > 0 {
				f = ReadVariable(f, parm)
				// }
				where = append(where, f)
			}
		}
	}

	//Check setNewHeader First
	if q.Connection().(*Connection).setNewHeader && (commandType != dbox.QueryPartInsert && commandType != dbox.QueryPartSave) {
		q.Connection().(*Connection).Close()
		filename := q.Connection().(*Connection).Info().Host
		os.Remove(filename)
		return errorlib.Error(packageName, "Query", modQuery, "Only Insert Permited")
	}

	q.Connection().(*Connection).TypeOpenFile = TypeOpenFile_Append
	if hasDelete || hasUpdate {
		q.Connection().(*Connection).TypeOpenFile = TypeOpenFile_Create
	}

	q.Connection().(*Connection).ExecOpr = false
	if !q.Connection().(*Connection).setNewHeader && (commandType != dbox.QueryPartSave || (commandType == dbox.QueryPartSave && q.Connection().(*Connection).writer == nil)) {
		e = q.Connection().(*Connection).StartSessionWrite()
		// toolkit.Printf("Debug 333 : %v \n\n", "masuk")
	}

	if e != nil {
		return errorlib.Error(packageName, "Query", modQuery, e.Error())
	}

	var execCond QueryCondition
	execCond.where = where

	switch commandType {
	case dbox.QueryPartSave:
		q.updatessave = false
		e = q.execQueryPartSave(data)
	case dbox.QueryPartInsert:
		e = q.execQueryPartInsert(data)
	case dbox.QueryPartDelete:
		e = q.execQueryPartDelete(execCond)
	case dbox.QueryPartUpdate:
		e = q.execQueryPartUpdate(data, execCond)
	}

	q.Connection().(*Connection).ExecOpr = true
	if e != nil {
		q.Connection().(*Connection).ExecOpr = false
	}

	if commandType != dbox.QueryPartSave || q.updatessave {
		e = q.Connection().(*Connection).EndSessionWrite()
		q.Connection().(*Connection).TypeOpenFile = TypeOpenFile_Append
	}

	if e != nil {
		return errorlib.Error(packageName, "Query", modQuery, e.Error())
	}

	return nil
}

func (q *Query) execQueryPartSave(dt toolkit.M) error {
	if len(dt) == 0 {
		return errorlib.Error(packageName, modQuery, "save", "data to insert is not found")
	}

	// writer := q.Connection().(*Connection).writer
	reader := q.Connection().(*Connection).reader
	tempHeader := []string{}

	for _, val := range q.Connection().(*Connection).headerColumn {
		tempHeader = append(tempHeader, val.name)
	}

	// Check ID Before Insert
	checkidfound := false
	if nameid := toolkit.IdField(dt); nameid != "" {
		q.updatessave = true

		var colsid int
		for i, val := range q.Connection().(*Connection).headerColumn {
			if val.name == nameid {
				colsid = i
			}
		}

		for {
			dataTempSearch, e := reader.Read()
			for i, val := range dataTempSearch {
				if i == colsid && val == dt[nameid] {
					checkidfound = true
					break
				}
			}
			if e == io.EOF {
				break
			} else if e != nil {
				return errorlib.Error(packageName, modQuery, "Save", e.Error())
			}
		}
	}

	if checkidfound {
		e := q.Connection().(*Connection).EndSessionWrite()
		if e != nil {
			return errorlib.Error(packageName, modQuery, "Save", e.Error())
		}

		q.Connection().(*Connection).TypeOpenFile = TypeOpenFile_Create

		e = q.Connection().(*Connection).StartSessionWrite()
		if e != nil {
			return errorlib.Error(packageName, modQuery, "Save", e.Error())
		}

		e = q.execQueryPartUpdate(dt, QueryCondition{})
		if e != nil {
			return errorlib.Error(packageName, modQuery, "Save", e.Error())
		}
		// time.Sleep(1000 * time.Millisecond)
	} else {
		//Change to Do Insert
		// dataTemp := []string{}

		// for _, v := range q.Connection().(*Connection).headerColumn {
		// 	if dt.Has(v.name) {
		// 		dataTemp = append(dataTemp, cast.ToString(dt[v.name]))
		// 	} else {
		// 		dataTemp = append(dataTemp, "")
		// 	}
		// }

		// if len(dataTemp) > 0 {
		// 	writer.Write(dataTemp)
		// 	writer.Flush()
		// }
		e := q.execQueryPartInsert(dt)
		if e != nil {
			return errorlib.Error(packageName, modQuery, "Save", e.Error())
		}
	}

	return nil
}

func (q *Query) execQueryPartInsert(dt toolkit.M) error {

	if len(dt) == 0 {
		return errorlib.Error(packageName, "Query", modQuery, "data to insert is not found")
	}

	writer := q.Connection().(*Connection).writer
	reader := q.Connection().(*Connection).reader
	dataTemp := []string{}
	// toolkit.Printf("Debug 465 : %v \n\n", q.Connection().(*Connection).setNewHeader)
	if q.Connection().(*Connection).setNewHeader {
		q.Connection().(*Connection).SetHeaderToolkitM(dt)
		q.Connection().(*Connection).setNewHeader = false

		for _, v := range q.Connection().(*Connection).headerColumn {
			dataTemp = append(dataTemp, v.name)
		}

		if len(dataTemp) > 0 {
			writer.Write(dataTemp)
			writer.Flush()
		}
		dataTemp = []string{}
	}

	// Check ID Before Insert
	if nameid := toolkit.IdField(dt); nameid != "" {
		var colsid int
		for i, val := range q.Connection().(*Connection).headerColumn {
			if val.name == nameid {
				colsid = i
			}
		}

		for {
			dataTempSearch, e := reader.Read()
			for i, val := range dataTempSearch {
				if i == colsid && val == dt[nameid] {
					return errorlib.Error(packageName, modQuery, "Insert", "Unique id is found")
				}
			}
			if e == io.EOF {
				break
			} else if e != nil {
				return errorlib.Error(packageName, modQuery, "Insert", e.Error())
			}
		}
	}

	for _, v := range q.Connection().(*Connection).headerColumn {
		if dt.Has(v.name) {
			dataTemp = append(dataTemp, cast.ToString(dt[v.name]))
		} else {
			dataTemp = append(dataTemp, "")
		}
	}

	if len(dataTemp) > 0 {
		writer.Write(dataTemp)
		writer.Flush()
	}

	return nil
}

func (q *Query) execQueryPartDelete(Cond QueryCondition) error {

	writer := q.Connection().(*Connection).writer
	reader := q.Connection().(*Connection).reader
	tempHeader := []string{}

	for _, val := range q.Connection().(*Connection).headerColumn {
		tempHeader = append(tempHeader, val.name)
	}

	for {
		foundDelete := true
		recData := toolkit.M{}

		dataTemp, e := reader.Read()
		for i, val := range dataTemp {
			recData.Set(tempHeader[i], val)
			if q.Connection().(*Connection).headerColumn[i].dataType == "int" {
				recData[tempHeader[i]] = cast.ToInt(val, cast.RoundingAuto)
			} else if q.Connection().(*Connection).headerColumn[i].dataType == "float" {
				recData[tempHeader[i]] = cast.ToF64(val, (len(val) - (strings.IndexAny(val, "."))), cast.RoundingAuto)
			}
		}

		foundDelete = Cond.getCondition(recData)

		if e == io.EOF {
			if !foundDelete && dataTemp != nil {
				writer.Write(dataTemp)
				writer.Flush()
			}
			break
		} else if e != nil {
			return errorlib.Error(packageName, modQuery, "Delete", e.Error())
		}

		if !foundDelete && dataTemp != nil {
			writer.Write(dataTemp)
			writer.Flush()
		}
	}

	return nil

}

func (q *Query) execQueryPartUpdate(dt toolkit.M, Cond QueryCondition) error {

	if len(dt) == 0 {
		return errorlib.Error(packageName, "Query", modQuery, "data to update is not found")
	}

	writer := q.Connection().(*Connection).writer
	reader := q.Connection().(*Connection).reader
	tempHeader := []string{}

	for _, val := range q.Connection().(*Connection).headerColumn {
		tempHeader = append(tempHeader, val.name)
	}

	for {
		foundChange := false

		recData := toolkit.M{}
		dataTemp, e := reader.Read()
		for i, val := range dataTemp {
			recData.Set(tempHeader[i], val)
			if q.Connection().(*Connection).headerColumn[i].dataType == "int" {
				recData[tempHeader[i]] = cast.ToInt(val, cast.RoundingAuto)
			} else if q.Connection().(*Connection).headerColumn[i].dataType == "float" {
				recData[tempHeader[i]] = cast.ToF64(val, (len(val) - (strings.IndexAny(val, "."))), cast.RoundingAuto)
			}
		}

		if len(Cond.where) > 0 { //|| (len(Cond.where) == 0 && toolkit.IdField(dt) == "") {
			foundChange = Cond.getCondition(recData)
		}

		// Check ID IF Condition Not Found
		if nameid := toolkit.IdField(dt); nameid != "" && !foundChange {
			if recData.Has(nameid) && dt[nameid] == recData[nameid] {
				foundChange = true
			}
		}

		if foundChange && len(dataTemp) > 0 {
			for n, v := range tempHeader {
				if dt.Has(v) {
					dataTemp[n] = cast.ToString(dt[v])
				}
			}
		}

		if e == io.EOF {
			if dataTemp != nil {
				writer.Write(dataTemp)
				writer.Flush()
			}
			break
		} else if e != nil {
			return errorlib.Error(packageName, modQuery, "Update", e.Error())
		}
		if dataTemp != nil {
			writer.Write(dataTemp)
			writer.Flush()
		}
	}

	return nil
}

func ReadVariable(f *dbox.Filter, in toolkit.M) *dbox.Filter {
	f.Field = strings.ToLower(f.Field)
	if (f.Op == "$and" || f.Op == "$or") &&
		strings.Contains(reflect.TypeOf(f.Value).String(), "dbox.Filter") {
		fs := f.Value.([]*dbox.Filter)
		/* nilai fs :  [0xc082059590 0xc0820595c0]*/
		for i, ff := range fs {
			/* nilai ff[0] : &{umur $gt @age} && ff[1] : &{name $eq @nama}*/
			bf := ReadVariable(ff, in)
			/* nilai bf[0] :  &{umur $gt 25} && bf[1] : &{name $eq Kane}*/
			fs[i] = bf
		}
		f.Value = fs
		return f
	} else {
		if reflect.TypeOf(f.Value).Kind() == reflect.Slice {
			if strings.Contains(reflect.TypeOf(f.Value).String(), "interface") {
				fSlice := f.Value.([]interface{})
				/*nilai fSlice : [@name1 @name2]*/
				for i := 0; i < len(fSlice); i++ {
					/* nilai fSlice [i] : @name1*/
					if string(cast.ToString(fSlice[i])[0]) == "@" {
						for key, val := range in {
							if cast.ToString(fSlice[i]) == key {
								fSlice[i] = val
							}
						}
					}
				}
				f.Value = fSlice
			} else if strings.Contains(reflect.TypeOf(f.Value).String(), "string") {
				fSlice := f.Value.([]string)
				for i := 0; i < len(fSlice); i++ {
					if string(fSlice[i][0]) == "@" {
						for key, val := range in {
							if fSlice[i] == key {
								fSlice[i] = val.(string)
							}
						}
					}
				}
				f.Value = fSlice
			}
			return f
		} else {
			if string(cast.ToString(f.Value)[0]) == "@" {
				for key, val := range in {
					if cast.ToString(f.Value) == key {
						f.Value = val
					}
				}
			}
			return f
		}
	}
	return f
}
