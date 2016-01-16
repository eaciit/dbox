package dbox

import (
	"github.com/eaciit/toolkit"
	"reflect"
	"strings"
	"time"
)

const (
	packageName   = "eaciit.dbox"
	modConnection = "Connection"
)

type DBOP string

const (
	DBINSERT  DBOP = "insert"
	DBUPDATE  DBOP = "update"
	DBDELETE  DBOP = "delete"
	DBSELECT  DBOP = "select"
	DBSAVE    DBOP = "save"
	DBCOMMAND DBOP = "command"
	DBUKNOWN  DBOP = "unknown"
)

func (d *DBOP) String() string {
	return string(*d)
}

func Find(ms []toolkit.M, filters []*Filter) (output []int) {
	for i, v := range ms {
		match := MatchM(v, filters)
		if match {
			output = append(output, i)
		}
	}
	return
}

func MatchM(v toolkit.M, filters []*Filter) bool {
	match := true

	for _, f := range filters {
		if f.Field != "" {
			//--- if has field: $eq, $ne, $gt, $lt, $gte, $lte, $contains
			if v.Has(f.Field) {
				match = match && MatchV(v.Get(f.Field), f)
			} else {
				if f.Op != FilterOpNoEqual && f.Op != FilterOpNin {
					return false
				}
			}
		} else {
			//-- no field: $and, $or
			if f.Op == FilterOpAnd || f.Op == FilterOpOr {
				filters2 := f.Value.([]*Filter)
				for _, f2 := range filters2 {
					if f.Op == FilterOpAnd {
						match = match && MatchM(v, []*Filter{f2})
					} else {
						match = match || MatchM(v, []*Filter{f2})
					}
				}
			}
		}
	}
	return match
}

func MatchV(v interface{}, f *Filter) bool {
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
	if toolkit.HasMember([]interface{}{FilterOpEqual, FilterOpNoEqual, FilterOpGt, FilterOpGte, FilterOpLt, FilterOpLte}, f.Op) {
		return Compare(v, f.Value, f.Op)
	} else if f.Op == FilterOpIn {
		var values []interface{}
		toolkit.FromBytes(toolkit.ToBytes(f.Value, ""), "", &values)
		return toolkit.HasMember(values, v)
	} else if f.Op == FilterOpNin {
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
		if op == FilterOpEqual {
			return vv1o == vv2o
		} else if op == FilterOpNoEqual {
			return vv1o != vv2o
		} else if op == FilterOpLt {
			return vv1o < vv2o
		} else if op == FilterOpLte {
			return vv1o <= vv2o
		} else if op == FilterOpGt {
			return vv1o > vv2o
		} else if op == FilterOpGte {
			return vv1o >= vv2o
		}
	} else if strings.Contains(t, "time.time") {
		//--- is a time.Time
		vv1o := vv1.Interface().(time.Time)
		vv2o := vv2.Interface().(time.Time)
		if op == FilterOpEqual {
			return vv1o == vv2o
		} else if op == FilterOpNoEqual {
			return vv1o != vv2o
		} else if op == FilterOpLt {
			return vv1o.Before(vv2o)
		} else if op == FilterOpLte {
			return vv1o == vv2o || vv1o.Before(vv2o)
		} else if op == FilterOpGt {
			return vv1o.After(vv2o)
		} else if op == FilterOpGte {
			return vv1o == vv2o || vv1o.After(vv2o)
		}

	} else {
		//--- will be string
		vv1o := vv1.Interface().(string)
		vv2o := vv2.Interface().(string)
		if op == FilterOpEqual {
			return vv1o == vv2o
		} else if op == FilterOpNoEqual {
			return vv1o != vv2o
		} else if op == FilterOpLt {
			return vv1o < vv2o
		} else if op == FilterOpLte {
			return vv1o <= vv2o
		} else if op == FilterOpGt {
			return vv1o > vv2o
		} else if op == FilterOpGte {
			return vv1o >= vv2o
		}
	}

	return false
}
