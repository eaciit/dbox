package json

import (
	"encoding/json"
	"github.com/eaciit/crowd"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"io/ioutil"
	"reflect"
	"strconv"
	"strings"
)

const (
	modQuery = "Query"
)

type Query struct {
	dbox.Query
	dataType            string
	hasNewSave, hasSave bool
	sliceData           []toolkit.M
}

func (q *Query) Prepare() error {
	return nil
}

func (q *Query) Cursor(in toolkit.M) (dbox.ICursor, error) {
	var (
		e        error
		dataMaps []toolkit.M
	)
	q.ReadFile(&dataMaps, q.Connection().(*Connection).filePath)
	cursor := dbox.NewCursor(new(Cursor))

	filters, e := q.Filters(in)
	if e != nil {
		return nil, errorlib.Error(packageName, modQuery, "Cursor", e.Error())
	}

	commandType := filters.GetString("cmdType")
	if commandType != dbox.QueryPartSelect {
		return nil, errorlib.Error(packageName, modQuery, "Cursor", "Cursor is only working with select command, for "+commandType+" please use .Exec instead")
	}

	aggregate := false
	hasWhere := filters.Has("where")

	if !aggregate {
		var whereFields []*dbox.Filter
		var dataInterface interface{}
		json.Unmarshal(toolkit.Jsonify(dataMaps), &dataInterface)

		if hasWhere {
			whereFields = filters.Get("where").([]*dbox.Filter)
			// jsonSelect = fields
			cursor.(*Cursor).isWhere = true
		}
		cursor = cursor.SetConnection(q.Connection())
		cursor.(*Cursor).whereFields = whereFields
		cursor.(*Cursor).jsonSelect = filters.Get("select").([]string)
		cursor.(*Cursor).readFile = toolkit.Jsonify(dataMaps)
	} else {
		return nil, errorlib.Error(packageName, modQuery, "Cursor", "No Aggregate function")
	}
	return cursor, nil
}

func (q *Query) Exec(parm toolkit.M) error {
	var (
		e                    error
		updatedValue, dataMs []toolkit.M
		dataM                toolkit.M
	)

	filters, e := q.Filters(parm)
	if e != nil {
		return errorlib.Error(packageName, modQuery, "Exec", e.Error())
	}

	if parm == nil {
		parm = toolkit.M{}
	}

	data := parm.Get("data", nil)
	filePath := q.Connection().(*Connection).filePath
	commandType := filters.Get("cmdType").(string)
	hasWhere := filters.Has("where")
	hasCmdType := toolkit.M{}
	hasData := parm.Has("data")
	getWhere := filters.Get("where", []*dbox.Filter{}).([]*dbox.Filter)
	dataIsSlice := toolkit.IsSlice(data)
	if dataIsSlice {
		e = toolkit.Unjson(toolkit.Jsonify(data), &dataMs)
		if e != nil {
			return errorlib.Error(packageName, modQuery, "Exec: "+commandType, "Data encoding error: "+e.Error())
		}
		for _, v := range dataMs {
			id := toolkit.Id(v)
			idF := toolkit.IdField(v)

			if toolkit.IsNilOrEmpty(id) {
				return errorlib.Error(packageName, modCursor+".Exec", commandType, "Unable to find ID in slice data")
			} else {
				getWhere = []*dbox.Filter{dbox.Eq(idF, id)}
			}
		}
	} else {
		dataM, e = toolkit.ToM(data)
		if e != nil {
			return errorlib.Error(packageName, modQuery, "Exec: "+commandType, "Unable to Map, error: "+e.Error())
		}

		id := toolkit.Id(dataM)
		if !toolkit.IsNilOrEmpty(id) {
			getWhere = []*dbox.Filter{dbox.Eq(toolkit.IdField(dataM), id)}
		}
	}

	var dataMaps []toolkit.M
	q.ReadFile(&dataMaps, filePath)

	if commandType == dbox.QueryPartInsert {
		hasCmdType.Set("hasInsert", true)

		if !hasData {
			return errorlib.Error(packageName, modCursor+".Exec", commandType, "Sorry data not found!, unable to insert data")
		}

		result := dbox.Find(dataMaps, getWhere)
		if len(result) > 0 {
			return errorlib.Error(packageName, modCursor+".Exec", commandType, "ID already exist, unable insert data ")
		}

		if dataIsSlice {
			var sliceData []toolkit.M
			for _, v := range dataMs {
				sliceData = finUpdateObj(dataMaps, v, "insert")
			}
			updatedValue = sliceData
		} else {
			updatedValue = finUpdateObj(dataMaps, dataM, "insert")
		}
	} else if commandType == dbox.QueryPartUpdate {
		hasCmdType.Set("hasUpdate", true)

		if !hasData {
			return errorlib.Error(packageName, modCursor+".Exec", commandType, "Sorry data not found!, unable to update data")
		}

		if hasWhere {
			var indexes []interface{}
			whereIndex := dbox.Find(dataMaps, getWhere)
			indexes = toolkit.ToInterfaceArray(&whereIndex)
			// toolkit.Printf("whereIndex>%v indexes%v\n", whereIndex, indexes)

			var dataUpdate toolkit.M
			var updateDataIndex int
			isDataSlice := toolkit.IsSlice(data)
			if isDataSlice == false {
				isDataSlice = false
				data, e = toolkit.ToM(data)
				if e != nil {
					return errorlib.Error(packageName, modQuery, "Exec: "+commandType, "Serde data fail"+e.Error())
				}

				e = toolkit.Serde(data, &dataUpdate, "")
				if e != nil {
					return errorlib.Error(packageName, modQuery, "Exec: "+commandType, "Serde data fail"+e.Error())
				}
			}

			for i, v := range dataMaps {
				if toolkit.HasMember(indexes, i) || !hasWhere {
					if isDataSlice {
						e = toolkit.Serde(toolkit.SliceItem(data, updateDataIndex), &dataUpdate, "")
						if e != nil {
							return errorlib.Error(packageName, modQuery, "Exec: "+commandType, "Serde data fail"+e.Error())
						}
						updateDataIndex++
					}

					dataOrigin := dataMaps[i]
					toolkit.CopyM(&dataUpdate, &dataOrigin, false, []string{"_id"})
					toolkit.Serde(dataOrigin, &v, "")
					dataMaps[i] = v
				}
			}
			updatedValue = dataMaps
		} else {
			updatedValue = finUpdateObj(dataMaps, dataM, "update")
		}
	} else if commandType == dbox.QueryPartDelete {
		hasCmdType.Set("hasDelete", true)
		// if multi {
		if hasWhere {
			result := dbox.Find(dataMaps, getWhere)
			if len(result) > 0 {
				for i, v := range dataMaps {
					if toolkit.HasMember(result, i) == false {
						updatedValue = append(updatedValue, v)
					}
				}
			}
		} else {
			updatedValue = []toolkit.M{}
		}
	} else if commandType == dbox.QueryPartSave {
		hasCmdType.Set("hasSave", true)
		if !hasData {
			return errorlib.Error(packageName, modCursor+".Exec", commandType, "Sorry data not found!, unable to update data")
		}

		q.dataType = "save"

		var r []toolkit.M
		toolkit.UnjsonFromString(toolkit.JsonString(getWhere), &r)
		for idx, _ := range r {
			// toolkit.Printf("idx>%v v>%v\n", idx, v)
			i := toolkit.SliceItem(r, idx)
			val, e := toolkit.ToM(i)
			if e != nil {
				return errorlib.Error(packageName, modCursor+".Exec", commandType, e.Error())
			}

			idString := ToString(reflect.ValueOf(val.Get("Value")).Kind(), val.Get("Value"))
			if q.Connection().(*Connection).sData == "" {
				q.Connection().(*Connection).sData = idString
			} else if q.Connection().(*Connection).sData == idString {
				q.Connection().(*Connection).sData = idString
				updatedValue = finUpdateObj(q.sliceData, dataM, "update")
				q.sliceData = updatedValue
			} else {
				q.Connection().(*Connection).sData = idString
				_ = finUpdateObj(q.sliceData, dataM, "update")
				q.sliceData = append(q.sliceData, dataM)
			}

			if len(q.sliceData) == 0 {
				q.sliceData = append(q.sliceData, dataM)
			}
		}
	}

	if hasCmdType.Has("hasInsert") || hasCmdType.Has("hasUpdate") || hasCmdType.Has("hasDelete") {
		e = q.WriteFile(updatedValue)
		if e != nil {
			return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
		}
	}

	return nil
}

func finUpdateObj(jsonData []toolkit.M, replaceData toolkit.M, isType string) []toolkit.M {
	var (
		mapVal []toolkit.M
	)

	if isType == "update" {
		iReplaceData := toolkit.Id(replaceData)
		reflectIs := reflect.ValueOf(iReplaceData).Kind()
		dataUptId := ToString(reflectIs, iReplaceData)

		for _, v := range jsonData {
			iSubV := toolkit.Id(v)
			reflectIs := reflect.ValueOf(iSubV).Kind()
			subvIdString := ToString(reflectIs, iSubV)
			if strings.ToLower(subvIdString) == strings.ToLower(dataUptId) {
				for key, _ := range v {
					delete(v, key)
				}

			}

			var newData = make(map[string]interface{})
			for i, dataUpt := range replaceData {
				newData[i] = dataUpt
			}
			for i, newSubV := range v {
				newData[i] = newSubV
			}
			mapVal = append(mapVal, newData)
		}
		return mapVal
	} else if isType == "insert" {
		val := append(jsonData, replaceData)
		return val
	}
	return nil

}

func (q *Query) ReadFile(Ms *[]toolkit.M, f string) error {
	readF, _ := ioutil.ReadFile(f)
	e := json.Unmarshal(readF, Ms)
	if e != nil {
		return errorlib.Error(packageName, modQuery+".Exec", "ReafFile", e.Error())
	}

	return nil
}

func (q *Query) WriteFile(newData []toolkit.M) error {
	if q.Connection().(*Connection).openFile == nil {
		q.Connection().(*Connection).OpenSession()
	}

	jValue, e := json.MarshalIndent(newData, "", "  ")
	if e != nil {
		return errorlib.Error(packageName, modQuery+".Exec", "Writefile", e.Error())
	}

	e = ioutil.WriteFile(q.Connection().(*Connection).filePath, jValue, 0666)
	if e != nil {
		return errorlib.Error(packageName, modQuery+".Exec", "Writefile", e.Error())
	}
	q.Connection().(*Connection).Close()
	return nil
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
	var e error
	var lastJson []toolkit.M

	q.ReadFile(&lastJson, q.Connection().(*Connection).filePath)
	if toolkit.SliceLen(lastJson) > 0 {
		var newData []toolkit.M //interface{}
		for _, v := range q.sliceData {
			if len(v) != 0 {
				newData = append(newData, v)
			}
		}

		var dataAfterUnset []toolkit.M
		for _, v := range lastJson {
			id := toolkit.Id(v)
			sId := ToString(reflect.ValueOf(id).Kind(), id)

			for _, vSlice := range newData {
				idLast := toolkit.Id(vSlice)
				sIdLast := ToString(reflect.ValueOf(idLast).Kind(), idLast)

				if strings.ToLower(sId) == strings.ToLower(sIdLast) {
					for k, _ := range v {
						v.Unset(k)
					}
				}
			}
			if len(v) != 0 {
				dataAfterUnset = append(dataAfterUnset, v)
			}
		}

		var allJsonData []toolkit.M
		allJsonData = append(dataAfterUnset, newData...)
		q.sliceData = allJsonData
	} else if q.hasSave {
		var newData []toolkit.M //interface{}
		for _, v := range q.sliceData {
			if len(v) != 0 {
				newData = append(newData, v)
			}
		}
		q.sliceData = newData
	}

	e = q.WriteFile(q.sliceData)
	if e != nil {
		return errorlib.Error(packageName, modQuery+".Exec", "HasPartExec", e.Error())
	}
	return nil
}

func (q *Query) Filters(parm toolkit.M) (toolkit.M, error) {
	filters := toolkit.M{}

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
		filters.Set("cmdType", dbox.QueryPartSelect)
	} else {
		_, hasDelete := parts[dbox.QueryPartDelete]
		_, hasInsert := parts[dbox.QueryPartInsert]
		_, hasUpdate := parts[dbox.QueryPartUpdate]
		_, hasSave := parts[dbox.QueryPartSave]

		if hasDelete {
			filters.Set("cmdType", dbox.QueryPartDelete)
		} else if hasInsert {
			filters.Set("cmdType", dbox.QueryPartInsert)
		} else if hasUpdate {
			filters.Set("cmdType", dbox.QueryPartUpdate)
		} else if hasSave {
			filters.Set("cmdType", dbox.QueryPartSave)
		} else {
			filters.Set("cmdType", dbox.QueryPartSelect)
		}
	}
	filters.Set("select", fields)

	whereParts, hasWhere := parts[dbox.QueryPartWhere]
	var where []*dbox.Filter
	if hasWhere {
		for _, p := range whereParts.([]interface{}) {
			fs := p.(*dbox.QueryPart).Value.([]*dbox.Filter)
			for _, f := range fs {
				where = append(where, f)
			}
		}
		filters.Set("where", where)
	}
	return filters, nil
}

func (q *Query) Close() {
	if q.dataType == "save" {
		q.HasPartExec()
	}

	if q.Connection().(*Connection).openFile != nil {
		q.Connection().(*Connection).Close()
	}

}
