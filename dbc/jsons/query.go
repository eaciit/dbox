package jsons

import (
	"github.com/eaciit/crowd"
	"github.com/eaciit/dbox"
	err "github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"
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
	where := in.Get("where", toolkit.M{}).(toolkit.M)

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
		if hasWhere {

		} else {

		}
	} else if commandType == dbox.QueryPartDelete {
		if hasData {

		} else if hasWhere {
			q.data = []toolkit.M{where}
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

func Find(ms []toolkit.M, filters []*dbox.Filter) (output []int) {
	for i, v := range ms {
		match := MatchM(v, filters)
		if match {
			output = append(output, i)
		}
	}
	return
}

func MatchM(v toolkit.M, filters []*dbox.Filter) bool {
	match := true

	for _, f := range filters {
		if f.Field != "" {
			//--- if has field: $eq, $ne, $gt, $lt, $gte, $lte, $contains
			if v.Has(f.Field) {
				match = match && MatchV(v.Get(f.Field), f)
			} else {
				if f.Op != dbox.FilterOpNoEqual && f.Op != dbox.FilterOpNin {
					return false
				}
			}
		} else {
			//-- no field: $and, $or
			if f.Op == dbox.FilterOpAnd || f.Op == dbox.FilterOpOr {
				filters2 := f.Value.([]*dbox.Filter)
				for _, f2 := range filters2 {
					if f.Op == dbox.FilterOpAnd {
						match = match && MatchM(v, []*dbox.Filter{f2})
					} else {
						match = match || MatchM(v, []*dbox.Filter{f2})
					}
				}
			}
		}
	}
	return match
}

func MatchV(v interface{}, f *dbox.Filter) bool {
	match := false
	/*
		rv0 := reflect.ValueOf(v)
		if rv0.Kind() == reflect.Ptr {
			rv0 = reflect.Indirect(rv0)
		}
		rv1 := reflect.ValueOf(f.Value)
		if rv1.Kind()==reflect.Ptr{
			rv1=reflect.Indirect(rv1)
		}
	*/
	if toolkit.HasMember([]interface{}{dbox.FilterOpEqual, dbox.FilterOpNoEqual, dbox.FilterOpGt, dbox.FilterOpGte, dbox.FilterOpLt, dbox.FilterOpLte}, f.Op) {
		return Compare(v, f.Value, f.Op)
	} else if f.Op == dbox.FilterOpIn {
		var values []interface{}
		toolkit.FromBytes(toolkit.ToBytes(f.Value, ""), "", &values)
		return toolkit.HasMember(values, v)
	} else if f.Op == dbox.FilterOpNin {
		var values []interface{}
		toolkit.FromBytes(toolkit.ToBytes(f.Value, ""), "", &values)
		return !toolkit.HasMember(values, v)
	}
	return match
}

func Compare(v1 interface{}, v2 interface{}, op string) bool {
	vv1 := reflect.Indirect(reflect.ValueOf(v1))
	vv2 := reflect.Indirect(reflect.ValueOf(v2))
	if vv1.Type().String() != vv2.Type().String() {
		return false
	}

	k := strings.ToLower(vv1.Kind().String())
	t := strings.ToLower(vv1.Type().String())
	if strings.Contains(k, "int") || strings.Contains(k, "float") {
		//--- is a number
		// lets convert all to float64 for simplicity
		var vv1o, vv2o float64
		if strings.Contains(k, "int") {
			vv1o = float64(vv1.Int())
			vv2o = float64(vv2.Int())
		} else {
			vv1o = vv1.Float()
			vv2o = vv2.Float()
		}
		if op == dbox.FilterOpEqual {
			return vv1o == vv2o
		} else if op == dbox.FilterOpNoEqual {
			return vv1o != vv2o
		} else if op == dbox.FilterOpLt {
			return vv1o < vv2o
		} else if op == dbox.FilterOpLte {
			return vv1o <= vv2o
		} else if op == dbox.FilterOpGt {
			return vv1o > vv2o
		} else if op == dbox.FilterOpGte {
			return vv1o >= vv2o
		}
	} else if strings.Contains(t, "time.time") {
		//--- is a time.Time
		vv1o := vv1.Interface().(time.Time)
		vv2o := vv2.Interface().(time.Time)
		if op == dbox.FilterOpEqual {
			return vv1o == vv2o
		} else if op == dbox.FilterOpNoEqual {
			return vv1o != vv2o
		} else if op == dbox.FilterOpLt {
			return vv1o.Before(vv2o)
		} else if op == dbox.FilterOpLte {
			return vv1o == vv2o || vv1o.Before(vv2o)
		} else if op == dbox.FilterOpGt {
			return vv1o.After(vv2o)
		} else if op == dbox.FilterOpGte {
			return vv1o == vv2o || vv1o.After(vv2o)
		}

	} else {
		//--- will be string
		vv1o := vv1.Interface().(string)
		vv2o := vv2.Interface().(string)
		if op == dbox.FilterOpEqual {
			return vv1o == vv2o
		} else if op == dbox.FilterOpNoEqual {
			return vv1o != vv2o
		} else if op == dbox.FilterOpLt {
			return vv1o < vv2o
		} else if op == dbox.FilterOpLte {
			return vv1o <= vv2o
		} else if op == dbox.FilterOpGt {
			return vv1o > vv2o
		} else if op == dbox.FilterOpGte {
			return vv1o >= vv2o
		}
	}

	return false
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
			return nil, err.Error(packageName, modQuery, "prepare",
				e.Error())
		} else {
			//fmt.Printf("Where: %s\n", toolkit.JsonString(where))
		}
		//where = iwhere.(toolkit.M)
	}
	output.Set("where", where)

	//data := toolkit.ToM(in.Get("data",nil))
	//output.Set("data",data)
	return
}
