package dbox

import (
	"github.com/eaciit/toolkit"
	//"reflect"
	//"strings"
	//"time"
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
	//toolkit.Printf("Find:%s Filter:%s\n", toolkit.JsonString(ms), toolkit.JsonString(filters))
	for i, v := range ms {
		match := MatchM(v, filters)
		if match {
			output = append(output, i)
		}
	}
	return
}

func MatchM(v toolkit.M, filters []*Filter) bool {
	var match bool

	for _, f := range filters {
		//toolkit.Printf("Filter:%s V:%s Has:%s Match:%s\n", toolkit.JsonString(f), toolkit.JsonString(v), v.Has(f.Field), match)
		if f.Field != "" {
			//--- if has field: $eq, $ne, $gt, $lt, $gte, $lte, $contains
			if v.Has(f.Field) {
				match = MatchV(v.Get(f.Field), f)
				//toolkit.Printf("Filter:%s Value: %v Match:%s \n", toolkit.JsonString(f), v.Get(f.Field), match)
				if !match {
					return false
				}
			} else {
				if f.Op != FilterOpNoEqual && f.Op != FilterOpNin {
					return false
				}
			}
		} else {
			//-- no field: $and, $or
			//toolkit.Printf("Filter: %s\n", toolkit.JsonString(f))
			if f.Op == FilterOpAnd || f.Op == FilterOpOr {
				filters2 := f.Value.([]*Filter)
				for k, f2 := range filters2 {
					if f.Op == FilterOpAnd {
						if k == 0 {
							match = MatchM(v, []*Filter{f2})
						} else {
							match = match && MatchM(v, []*Filter{f2})
						}
					} else {
						if k == 0 {
							match = MatchM(v, []*Filter{f2})
						} else {
							match = match || MatchM(v, []*Filter{f2})
						}
					}
				}
			}
			//toolkit.Printf("\n")
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
		return toolkit.Compare(v, f.Value, f.Op)
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
