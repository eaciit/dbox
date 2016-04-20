package json

import (
	"encoding/json"
	"github.com/eaciit/crowd"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"io/ioutil"
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
	whereData           []*dbox.Filter
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
	cursor = cursor.SetConnection(q.Connection())

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
	hasAggregate := filters.Get("aggregate").(bool)
	if hasAggregate {
		aggregate = true
	}

	if !aggregate {
		if hasWhere {
			// toolkit.Println("where:", toolkit.JsonString(filters.Get("where")))
			cursor.(*Cursor).whereFields = filters.Get("where").([]*dbox.Filter)
			cursor.(*Cursor).isWhere = true
			cursor.(*Cursor).indexes = dbox.Find(dataMaps, filters.Get("where", []*dbox.Filter{}).([]*dbox.Filter))
		}
		// toolkit.Println("skip:", toolkit.JsonString(filters.Get("skip")))
		skip := 0
		if skip = filters.Get("skip").(int); skip > 0 {
			cursor.(*Cursor).skip = skip
		}
		// toolkit.Println("take:", toolkit.JsonString(filters.Get("take")))
		take := 0
		if take = filters.Get("take").(int); take > 0 {
			cursor.(*Cursor).take = take
		}

		if sort := filters.Get("sort").([]string); toolkit.SliceLen(sort) > 0 {
			fb := new(FilterBuilder)
			// toolkit.Printf("sorter:%v\n", sort)
			sorter := fb.SortFetch(sort, dataMaps)

			cursor.(*Cursor).datas = sorter
		} else {
			cursor.(*Cursor).datas = dataMaps
		}
		var count int
		if hasWhere {
			count = toolkit.SliceLen(cursor.(*Cursor).indexes)
		} else {
			count = toolkit.SliceLen(cursor.(*Cursor).datas)
		}
		if count <= skip {
			count = 0
		} else {
			count -= skip
		}
		if count >= take && take > 0 {
			count = take
		}
		cursor.(*Cursor).count = count

		cursor.(*Cursor).jsonSelect = filters.Get("select").([]string)
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
		if hasWhere {
			result := dbox.Find(dataMaps, getWhere)
			if len(result) > 0 || len(result) > 1 {
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
		q.whereData = append(q.whereData, getWhere...)
		q.sliceData = append(q.sliceData, dataM)
		q.HasPartExec()
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
		//reflectIs := reflect.ValueOf(iReplaceData).Kind()
		//dataUptId := ToString(reflectIs, iReplaceData)
		dataUptId := toolkit.ToString(iReplaceData)

		for _, v := range jsonData {
			iSubV := toolkit.Id(v)
			//reflectIs := reflect.ValueOf(iSubV).Kind()
			subvIdString := toolkit.ToString(iSubV)
			if strings.ToLower(subvIdString) == strings.ToLower(dataUptId) {
				for key := range v {
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

func (q *Query) HasPartExec() error {
	var e error
	var lastJson []toolkit.M

	q.ReadFile(&lastJson, q.Connection().(*Connection).filePath)
	if toolkit.SliceLen(lastJson) > 0 {
		getWhere := []*dbox.Filter{}
		for _, v := range q.whereData {
			getWhere = []*dbox.Filter{v}
			i := dbox.Find(q.sliceData, getWhere)

			for idSlice := range q.sliceData {
				if toolkit.HasMember(i, idSlice) {
					idata := dbox.Find(lastJson, getWhere)
					for idx := range lastJson {
						if toolkit.HasMember(idata, idx) {
							lastJson[idx] = q.sliceData[idSlice]
						}
					}
					if toolkit.SliceLen(idata) == 0 {
						lastJson = append(lastJson, q.sliceData[idSlice])
					}
				}
			}
		}
		q.sliceData = lastJson
	} else {
		idx := []int{}
		for _, v := range q.whereData {
			getWhere := []*dbox.Filter{v}
			idx = dbox.Find(q.sliceData, getWhere)

		}
		// toolkit.Printf("newdata>%v\n", idx)
		if toolkit.SliceLen(idx) > 1 {
			newdata := toolkit.M{}
			for idslice, dataslice := range q.sliceData {
				if toolkit.HasMember(idx, idslice) {
					idf, _ := toolkit.IdInfo(dataslice)
					newdata = q.sliceData[idslice]
					toolkit.CopyM(&dataslice, &newdata, false, []string{idf})
				}
			}
			q.sliceData = []toolkit.M{}
			q.sliceData = append(q.sliceData, newdata)
		}

	}

	e = q.WriteFile(q.sliceData)
	if e != nil {
		return errorlib.Error(packageName, modQuery+".Exec", "HasPartExec", e.Error())
	}
	return nil
}

func (q *Query) Filters(parm toolkit.M) (toolkit.M, error) {
	filters := toolkit.M{}

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
	if skipPart, hasSkip := parts[dbox.QueryPartSkip]; hasSkip {
		skip = skipPart.([]*dbox.QueryPart)[0].Value.(int)
	}
	filters.Set("skip", skip)

	take := 0
	if takeParts, hasTake := parts[dbox.QueryPartTake]; hasTake {
		take = takeParts.([]*dbox.QueryPart)[0].Value.(int)
	}
	filters.Set("take", take)

	var aggregate bool
	if _, hasAggr := parts[dbox.QueryPartAggr]; hasAggr {
		aggregate = true
	}
	filters.Set("aggregate", aggregate)

	var sort []string
	if sortParts, hasSort := parts[dbox.QueryPartOrder]; hasSort {
		sort = []string{}
		for _, sl := range sortParts.([]*dbox.QueryPart) {
			// qp := sl.(*dbox.QueryPart)
			for _, fid := range sl.Value.([]string) {
				sort = append(sort, fid)
			}
		}
	}
	filters.Set("sort", sort)

	var fields []string
	selectParts, hasSelect := parts[dbox.QueryPartSelect]
	if hasSelect {
		// fields = toolkit.M{}
		for _, sl := range selectParts.([]*dbox.QueryPart) {
			// qp := sl.(*dbox.QueryPart)
			for _, fid := range sl.Value.([]string) {
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
		fb := new(FilterBuilder)
		for _, p := range whereParts.([]*dbox.QueryPart) {
			fs := p.Value.([]*dbox.Filter)
			for _, f := range fs {
				f := fb.CheckFilter(f, parm)

				where = append(where, f)
			}
		}
		filters.Set("where", where)
	}
	// toolkit.Printf("where>%v\n", toolkit.JsonString(filters.Get("where")))

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
