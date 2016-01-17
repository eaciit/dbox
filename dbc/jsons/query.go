package jsons

import (
	"github.com/eaciit/crowd"
	"github.com/eaciit/dbox"
	err "github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"path/filepath"
	"strings"
	"sync"
)

type Query struct {
	dbox.Query
	sync.Mutex

	jsonPath string
	data     []toolkit.M
}

func (q *Query) Cursor(in toolkit.M) (dbox.ICursor, error) {
	var cursor *Cursor

	setting, e := q.prepare(in)
	if e != nil {
		return nil, err.Error(packageName, modQuery, "Cursor", e.Error())
	}

	if setting.GetString("commandtype") != dbox.QueryPartSelect {
		return nil, err.Error(packageName, modQuery, "Cursor", "Cursor is only working with select command, please use .Exec instead")
	}

	cursor = newCursor(q)
	return cursor, nil
}

func (q *Query) Exec(in toolkit.M) error {
	setting, e := q.prepare(in)
	commandType := setting["commandtype"].(string)
	if e != nil {
		return err.Error(packageName, modQuery, "Exec: "+commandType, e.Error())
	}

	if setting.GetString("commandtype") == dbox.QueryPartSelect {
		return err.Error(packageName, modQuery, "Exec: "+commandType, "Exec is not working with select command, please use .Cursor instead")
	}

	q.Lock()
	defer q.Unlock()

	var dataM toolkit.M
	var dataMs []toolkit.M

	hasData := in.Has("data")
	dataIsSlice := false
	data := in.Get("data")
	if toolkit.IsSlice(data) {
		dataIsSlice = true
		e = toolkit.Unjson(toolkit.Jsonify(data), dataMs)
		if e != nil {
			return err.Error(packageName, modQuery, "Exec: "+commandType, "Data encoding error: "+e.Error())
		}
	} else {
		dataM, e = toolkit.ToM(data)
		dataMs = append(dataMs, dataM)
		if e != nil {
			return err.Error(packageName, modQuery, "Exec: "+commandType, "Data encoding error: "+e.Error())
		}
	}

	hasWhere := in.Has("where")
	where := in.Get("where", []*dbox.Filter{}).([]*dbox.Filter)

	if hasData && hasWhere == false && toolkit.HasMember([]interface{}{dbox.QueryPartInsert, dbox.QueryPartUpdate, dbox.QueryPartSave}, commandType) {
		hasWhere = true
		if toolkit.IsSlice(data) {
			ids := []interface{}{}
			idField := ""
			if idField == "" {
				return err.Error(packageName, modQuery, "Exec:"+commandType, "Data send is a slice, but its element has no ID")
			}
			dataCount := toolkit.SliceLen(data)
			for i := 0; i < dataCount; i++ {
				dataI := toolkit.SliceItem(data, i)
				if i == 0 {
					idField = toolkit.IdField(dataI)
				}
				ids = append(ids, toolkit.Id(dataI))
			}
			where = []*dbox.Filter{dbox.In(idField, ids)}
		} else {
			id := toolkit.Id(data)
			if toolkit.IsNilOrEmpty(id) {
				where = []*dbox.Filter{dbox.Eq(toolkit.IdField(id), id)}
			} else {
				where = nil
				hasWhere = false
			}
		}
	}

	q.openFile()
	if commandType == dbox.QueryPartInsert {
		if !hasData {
			return err.Error(packageName, modQuery, "Exec:"+commandType, "Data is empty")
		}
		if dataIsSlice {
			q.data = append(q.data, dataMs...)
		} else {
			q.data = append(q.data, dataM)
		}
	} else if commandType == dbox.QueryPartUpdate {
		if !hasData {
			return err.Error(packageName, modQuery, "Exec:"+commandType, "Data is empty")
		}

		var indexes []interface{}
		if hasWhere {
			toolkit.Serde(dbox.Find(q.data, where), &indexes, "")
		}

		var dataUpdate toolkit.M
		var updateDataIndex int

		isDataSlice := toolkit.IsSlice(data)
		if isDataSlice == false {
			isDataSlice = false
			e = toolkit.Serde(data, &dataUpdate, "")
			if e != nil {
				return err.Error(packageName, modQuery, "Exec:"+commandType, "Unable to serialize data. "+e.Error())
			}
		}
		var idField string
		for i, v := range q.data {
			if toolkit.HasMember(indexes, i) || len(indexes) == 0 {
				if idField == "" {
					idField = toolkit.IdField(v)
					if idField == "" {
						return err.Error(packageName, modQuery, "Exec:"+commandType, "No ID")
					}
				}

				var dataOrigin toolkit.M
				e = toolkit.Serde(v, &dataOrigin, "")
				if e != nil {
					return err.Error(packageName, modQuery, "Exec:"+commandType, "Unable to serialize data origin. "+e.Error())
				}
				if isDataSlice {
					e = toolkit.Serde(toolkit.SliceItem(data, updateDataIndex), &dataUpdate, "")
					if e != nil {
						return err.Error(packageName, modQuery, "Exec:"+commandType, "Unable to serialize data. "+e.Error())
					}
					updateDataIndex++
				}
				for fieldName, fieldValue := range dataUpdate {
					if fieldName != idField {
						if dataOrigin.Has(fieldName) {
							dataOrigin.Set(fieldName, fieldValue)
						}

					}
				}
				toolkit.Serde(dataOrigin, &v, "")
				q.data[i] = v
			}
		}
	} else if commandType == dbox.QueryPartDelete {
		if hasWhere {
			var indexes []interface{}
			toolkit.Serde(dbox.Find(q.data, where), &indexes, "")
			if len(indexes) > 0 {
				newdata := []toolkit.M{}
				for index, v := range q.data {
					if toolkit.HasMember(indexes, index) == false {
						newdata = append(newdata, v)
					}
				}
				q.data = newdata
			}
		} else {
			q.data = []toolkit.M{}
		}
	} else if commandType == dbox.QueryPartSave {
		if !hasData {
			return err.Error(packageName, modQuery, "Exec:"+commandType, "Data is empty")
		}
	}
	q.writeFile()
	return nil
}

func (q *Query) Close() {
}

func (q *Query) openFile() error {
	return nil
}

func (q *Query) writeFile() error {
	return nil
}

func (q *Query) prepare(in toolkit.M) (output toolkit.M, e error) {
	output = toolkit.M{}
	parts := crowd.From(q.Parts()).Group(func(x interface{}) interface{} {
		qp := x.(*dbox.QueryPart)
		return qp.PartType
	}, nil).Data

	//return nil, errorlib.Error(packageName, modQuery, "prepare", "asdaa")
	//fmt.Printf("Query parts: %s\n", toolkit.JsonString(q.Parts()))
	fromParts, hasFrom := parts[dbox.QueryPartFrom]
	if hasFrom == false {
		return nil, err.Error(packageName, "Query", "prepare", "Invalid table name")
	}
	tablename := fromParts.([]interface{})[0].(*dbox.QueryPart).Value.(string)
	output.Set("tablename", tablename)
	q.jsonPath = filepath.Join(q.Connection().(*Connection).Folder, tablename+".json")

	skip := 0
	if skipParts, hasSkip := parts[dbox.QueryPartSkip]; hasSkip {
		skip = skipParts.([]interface{})[0].(*dbox.QueryPart).
			Value.(int)
	}
	output.Set("skip", skip)

	take := 0
	if takeParts, has := parts[dbox.QueryPartTake]; has {
		take = takeParts.([]interface{})[0].(*dbox.QueryPart).
			Value.(int)
	}
	output.Set("take", take)

	var aggregate bool
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
			aggrExpression.Set(aggr.Alias, toolkit.M{}.Set(aggr.Op, aggr.Field))
		}
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

	output.Set("aggregate", aggregate)
	output.Set("aggrExpression", aggrExpression)

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
		output.Set("commandtype", dbox.QueryPartSelect)
	} else {
		_, hasUpdate := parts[dbox.QueryPartUpdate]
		_, hasInsert := parts[dbox.QueryPartInsert]
		_, hasDelete := parts[dbox.QueryPartDelete]
		_, hasSave := parts[dbox.QueryPartSave]

		if hasInsert {
			output.Set("commandtype", dbox.QueryPartInsert)
		} else if hasUpdate {
			output.Set("commandtype", dbox.QueryPartUpdate)
		} else if hasDelete {
			output.Set("commandtype", dbox.QueryPartDelete)
		} else if hasSave {
			output.Set("commandtype", dbox.QueryPartSave)
		} else {
			output.Set("commandtype", dbox.QueryPartSelect)
		}
	}
	output.Set("fields", fields)

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
	output.Set("sort", sort)

	var filters []*dbox.Filter
	whereParts, hasWhere := parts[dbox.QueryPartWhere]
	if hasWhere {
		for _, p := range whereParts.([]interface{}) {
			fs := p.(*dbox.QueryPart).Value.([]*dbox.Filter)
			for _, f := range fs {
				filters = append(filters, f)
			}
		}
	}
	output.Set("where", filters)
	return
}
