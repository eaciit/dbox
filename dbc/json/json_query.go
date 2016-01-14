package json

import (
	"encoding/json"
	"fmt"
	"github.com/eaciit/crowd"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	// "io"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"strings"
)

const (
	modQuery = "Query"
)

type Query struct {
	dbox.Query
	filePath, dataType  string
	session             *os.File
	hasNewSave, hasSave bool
	sliceData           toolkit.Ms
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
	t, _ := ioutil.ReadFile(q.Connection().(*Connection).filePath)
	cursor := dbox.NewCursor(new(Cursor))
	cursor = cursor.SetConnection(q.Connection())
	cursor.(*Cursor).readFile = t

	/*
		parts will return E - map{interface{}}interface{}
		where each interface{} returned is slice of interfaces --> []interface{}
	*/
	parts := crowd.From(q.Parts()).Group(func(x interface{}) interface{} {
		qp := x.(*dbox.QueryPart)
		return qp.PartType
	}, nil).Data

	// var fields toolkit.M
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

	// //where := toolkit.M{}
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
		}
	}

	if !aggregate {
		var whereFields, jsonSelect interface{}
		var dataInterface interface{}
		json.Unmarshal(t, &dataInterface)
		count, ok := dataInterface.([]interface{})

		if !ok {
			return nil, errorlib.Error(packageName,
				modQuery, "Cursor", "the file contains invalid json data")
		}
		cursor.(*Cursor).count = len(count)
		if fields != nil {
			q.Connection().(*Connection).FetchSession()
			jsonSelect = fields
		}
		if where != nil {
			whereFields = where
			jsonSelect = fields
			cursor.(*Cursor).isWhere = true
		}
		cursor.(*Cursor).tempPathFile = q.Connection().(*Connection).tempPathFile

		// cursor.(*Cursor).ResultType = QueryResultCursor
		cursor.(*Cursor).whereFields = whereFields
		cursor.(*Cursor).jsonSelect = jsonSelect
	} else {

		// cursor.(*Cursor).ResultType = QueryResultPipe
	}
	return cursor, nil
}

func (q *Query) Exec(parm toolkit.M) error {
	var e error

	if parm == nil {
		parm = toolkit.M{}
	}

	data := parm.Get("data", nil)
	filePath := q.Connection().(*Connection).filePath
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

		//
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
				return errorlib.Error(packageName, modQuery, "Cursor",
					e.Error())
			} else {
				//fmt.Printf("Where: %s", toolkit.JsonString(where))
			}
		}
	} else {
		if where == nil {
			id := toolkit.Id(data)
			if id != nil {
				where = (toolkit.M{}).Set("id", id)
			}
		} else {
			multi = true
		}
	}

	if commandType == dbox.QueryPartInsert {
		var jsonUpdatedValue []byte
		if reflect.ValueOf(data).Kind() == reflect.Slice {
			readF, _ := ioutil.ReadFile(filePath)

			var dataMap []map[string]interface{}
			e := json.Unmarshal(readF, &dataMap)
			if e != nil {
				return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
			}

			j, err := json.Marshal(data)
			if err != nil {
				return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
			}

			var jToMap []toolkit.M
			e = json.Unmarshal(j, &jToMap)

			var sliceData []map[string]interface{}
			for _, v := range jToMap {
				// sliceData = append(dataMap, v)
				_, sliceData = finUpdateObj(dataMap, v, "insert")
			}

			jsonUpdatedValue, err = json.MarshalIndent(sliceData, "", "  ")
			if err != nil {
				return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
			}
		} else {
			readF, e := ioutil.ReadFile(filePath)
			if e != nil {
				return errorlib.Error(packageName, modCursor+".Exec", commandType, e.Error())
			}

			var dataMap []map[string]interface{}
			e = json.Unmarshal(readF, &dataMap)
			if e != nil {
				return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
			}
			dataToMap, e := toolkit.ToM(data)
			if e != nil {
				return errorlib.Error(packageName, modCursor+".Exec", commandType, e.Error())
			}

			_, updatedValue := finUpdateObj(dataMap, dataToMap, "insert")
			jsonUpdatedValue, e = json.MarshalIndent(updatedValue, "", "  ")
			if e != nil {
				return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
			}
		}

		q.Connection().(*Connection).WriteSession()
		i, e := q.Connection().(*Connection).openFile.Write(jsonUpdatedValue) //t.WriteString(string(j))
		if i == 0 || e != nil {
			return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
		}

		err := q.Connection().(*Connection).CloseWriteSession()
		if err != nil {
			return errorlib.Error(packageName, modQuery+".Exec", commandType, err.Error())
		}
	} else if commandType == dbox.QueryPartUpdate {
		if multi {
			// 		_, e = mgoColl.UpdateAll(where, data)
		} else {
			readF, e := ioutil.ReadFile(filePath)
			if e != nil {
				return errorlib.Error(packageName, modCursor+".Exec", commandType, e.Error())
			}

			var dataMap []map[string]interface{}
			e = json.Unmarshal(readF, &dataMap)
			if e != nil {
				return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
			}
			a, e := toolkit.ToM(data)
			if e != nil {
				return errorlib.Error(packageName, modCursor+".Exec", commandType, e.Error())
			}

			updatedValue, _ := finUpdateObj(dataMap, a, "update")

			jsonUpdatedValue, err := json.MarshalIndent(updatedValue, "", "  ")
			if err != nil {
				return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
			}

			err = q.Connection().(*Connection).WriteSession()
			if err != nil {
				return errorlib.Error(packageName, modQuery+".Exec", commandType, err.Error())
			}

			i, e := q.Connection().(*Connection).openFile.Write(jsonUpdatedValue) //t.WriteString(string(j))
			if i == 0 || e != nil {
				return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
			}

			err = q.Connection().(*Connection).CloseWriteSession()
			if err != nil {
				return errorlib.Error(packageName, modQuery+".Exec", commandType, err.Error())
			}
		}
	} else if commandType == dbox.QueryPartDelete {
		if multi {
			if where != nil {
				readF, e := ioutil.ReadFile(filePath)
				if e != nil {
					return errorlib.Error(packageName, modCursor+".Exec", commandType, e.Error())
				}

				var dataMap []map[string]interface{}
				e = json.Unmarshal(readF, &dataMap)
				if e != nil {
					return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
				}
				a, e := toolkit.ToM(where)
				if e != nil {
					return errorlib.Error(packageName, modCursor+".Exec", commandType, e.Error())
				}

				updatedValue, _ := finUpdateObj(dataMap, a, "deleteMulti")

				jsonUpdatedValue, err := json.MarshalIndent(updatedValue, "", "  ")
				if err != nil {
					return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
				}

				err = q.Connection().(*Connection).WriteSession()
				if err != nil {
					return errorlib.Error(packageName, modQuery+".Exec", commandType, err.Error())
				}

				if string(jsonUpdatedValue) == "null" {
					ioutil.WriteFile(q.Connection().(*Connection).filePath, []byte("[\n]"), 0666)
				} else {
					i, e := q.Connection().(*Connection).openFile.Write(jsonUpdatedValue) //t.WriteString(string(j))
					if i == 0 || e != nil {
						return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
					}

					err = q.Connection().(*Connection).CloseWriteSession()
					if err != nil {
						return errorlib.Error(packageName, modQuery+".Exec", commandType, err.Error())
					}
				}
			} else {
				e := os.Remove(filePath)
				if e != nil {
					return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
				}

				_, err := os.Stat(filePath)
				if os.IsNotExist(err) {
					cf, e := os.Create(filePath)
					if e != nil {
						return errorlib.Error(packageName, modCursor+".Exec", commandType, e.Error())
					}
					cf.Close()
				}
				ioutil.WriteFile(q.Connection().(*Connection).filePath, []byte("[\n]"), 0666)
			}
		} else {

		}
	} else if commandType == dbox.QueryPartSave {
		dataType := reflect.ValueOf(data).Kind()

		if reflect.Slice == dataType {
			if q.Connection().(*Connection).openFile == nil {
				q.Connection().(*Connection).OpenSession()
			}

			if q.Connection().(*Connection).isNewSave {
				j, err := json.MarshalIndent(data, "", "  ")
				if err != nil {
					return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
				}

				i, e := q.Connection().(*Connection).openFile.Write(j) //t.WriteString(string(j))
				if i == 0 || e != nil {
					return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
				}
			} else {
				readF, e := ioutil.ReadFile(filePath)
				if e != nil {
					return errorlib.Error(packageName, modCursor+".Exec", commandType, e.Error())
				}

				var dataMap, newData []map[string]interface{}
				if e := json.Unmarshal(readF, &dataMap); e != nil {
					return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
				}

				v := toolkit.JsonString(data)
				if e = json.Unmarshal([]byte(v), &newData); e != nil {
					return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
				}

				mergeData := append(dataMap, newData...)

				jsonUpdatedValue, err := json.MarshalIndent(mergeData, "", "  ")
				if err != nil {
					return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
				}

				e = ioutil.WriteFile(filePath, jsonUpdatedValue, 0666)
				if e != nil {
					return errorlib.Error(packageName, modQuery+".Exec", "Write file", e.Error())
				}
			}
		} else if reflect.Struct == dataType {
			if q.Connection().(*Connection).openFile == nil {
				q.Connection().(*Connection).OpenSession()
			}
			q.dataType = "struct"
			dataMap, e := toolkit.ToM(data)
			if e != nil {
				return errorlib.Error(packageName, modCursor+".Exec", commandType, e.Error())
			}
			q.sliceData = append(q.sliceData, dataMap)

			if q.Connection().(*Connection).isNewSave {
				q.hasNewSave = hasSave
			} else {
				q.hasSave = hasSave
			}
		}
	}

	if e != nil {
		return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
	}

	return nil
}

func finUpdateObj(jsonData []map[string]interface{}, replaceData toolkit.M, isType string) ([]toolkit.M, []map[string]interface{}) {
	var (
		remMap map[string]interface{}
		mapVal []toolkit.M
	)

	if isType == "update" {
		iReplaceData := toolkit.Id(replaceData)
		reflectIs := reflect.ValueOf(iReplaceData).Kind()
		dataUptId := fmt.Sprintf("%s", ToString(reflectIs, iReplaceData))

		for _, v := range jsonData {
			iSubV := toolkit.Id(v)
			reflectIs := reflect.ValueOf(iSubV).Kind()
			subvIdString := fmt.Sprintf("%s", ToString(reflectIs, iSubV))
			if strings.ToLower(subvIdString) == strings.ToLower(dataUptId) {
				for key, _ := range v {
					delete(v, key)
				}

			}
			// if len(v) != 0 {
			var newData = make(map[string]interface{})
			for i, dataUpt := range replaceData {
				newData[i] = dataUpt
			}
			for i, newSubV := range v {
				newData[i] = newSubV
			}
			mapVal = append(mapVal, newData)
			// }
		}
		// mapVal = append(mapVal, replaceData)

		return mapVal, nil
	} else if isType == "insert" {
		val := append(jsonData, replaceData)
		return nil, val
	} else if isType == "deleteMulti" {
		for _, v := range jsonData {
			for _, subV := range v {
				reflectIs := reflect.ValueOf(subV).Kind()
				subvIdString := fmt.Sprintf("%s", ToString(reflectIs, subV))

				for _, dataUpt := range replaceData {
					reflectIs := reflect.ValueOf(dataUpt).Kind()
					dataUptId := fmt.Sprintf("%s", ToString(reflectIs, dataUpt))

					if strings.ToLower(dataUptId) == strings.ToLower(subvIdString) {
						remMap = v
						break
					}
				}

				for key, remVal := range remMap {
					delete(v, key)

					if reflect.ValueOf(subV).Kind() == reflect.String && reflect.ValueOf(remVal).Kind() == reflect.String {
						if strings.ToLower(remVal.(string)) == strings.ToLower(subV.(string)) {
							break
						}
					}
				}
			}

			if len(v) != 0 {
				var newData = make(map[string]interface{})
				for i, newSubV := range v {
					newData[i] = newSubV
				}
				mapVal = append(mapVal, newData)
			} /*else {
				var emptySlice toolkit.M
				mapVal = append(mapVal, emptySlice)
			}*/

		}
		return mapVal, nil
	}
	return nil, nil

}

func ToString(reflectIs reflect.Kind, i interface{}) string {
	var s string
	if reflectIs != reflect.String {
		toI := toolkit.ToInt(i)
		s = strconv.Itoa(toI)
	} else {
		s = i.(string)
	}
	return s
}

func (q *Query) HasPartExec() error {
	var jsonString []byte
	var err error
	if q.hasNewSave {
		jsonString, err = json.MarshalIndent(q.sliceData, "", "  ")
		if err != nil {
			return errorlib.Error(packageName, modQuery+".Exec", "Has part exec Marshal JSON", err.Error())
		}
	} else if q.hasSave {
		lastJson := q.Connection().(*Connection).getJsonToMap
		var allJsonData toolkit.Ms
		allJsonData = append(lastJson, q.sliceData...)

		jsonString, err = json.MarshalIndent(allJsonData, "", "  ")
		if err != nil {
			return errorlib.Error(packageName, modQuery+".Exec", "Has part exec Marshal JSON", err.Error())
		}
	}

	err = ioutil.WriteFile(q.Connection().(*Connection).filePath, jsonString, 0666)
	if err != nil {
		return errorlib.Error(packageName, modQuery+".Exec", "Write file", err.Error())
	}
	return nil
}

func (q *Query) Close() {
	if q.dataType == "struct" {
		q.HasPartExec()
	}

	if q.Connection().(*Connection).openFile != nil {
		q.Connection().(*Connection).Close()
	}

}
