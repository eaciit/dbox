package dbox

import (
	"github.com/eaciit/toolkit"
	//"reflect"
	"strings"
	//"time"
	"regexp"
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

//func Find(ms []toolkit.M, filters []*Filter) (output []int) {
func Find(ms interface{}, filters []*Filter) (output []int) {
	//-- is not a slice
	if !toolkit.IsSlice(ms) {
		toolkit.Println("Data is not slice")
		return []int{}
	}

	//toolkit.Printf("Find:%s Filter:%s\n", toolkit.JsonString(ms), toolkit.JsonString(filters))
	sliceLen := toolkit.SliceLen(ms)
	for i := 0; i < sliceLen; i++ {
		var v toolkit.M
		item := toolkit.SliceItem(ms, i)
		e := toolkit.Serde(item, &v, "json")
		if e == nil {
			match := MatchM(v, filters)
			if match {
				output = append(output, i)
			}
		} else {
			//toolkit.Println("Serde Fail: ", e.Error(), " Data: ", item)
		}
	}
	return
}

func CheckValue(v toolkit.M, f *Filter) (bool, interface{}) {
	resbool := false
	filedtemp := []interface{}{}

	resbool = v.Has(f.Field)
	if resbool {
		return resbool, v.Get(f.Field)
	} else if strings.Contains(f.Field, ".") {
		ar := strings.Split(f.Field, ".")
		for i, dt := range ar {
			if i == 0 {
				resbool = v.Has(dt)
				if !resbool {
					break
				} else {
					filedtemp = append(filedtemp, v.Get(dt))
				}
			} else {
				temp := toolkit.M(filedtemp[i-1].(map[string]interface{}))
				resbool = temp.Has(dt)
				if !resbool {
					break
				} else {
					filedtemp = append(filedtemp, temp.Get(dt))
				}
			}

			if i == len(ar)-1 {
				return true, filedtemp[i]
			}
		}
	}
	return false, nil
}

func MatchM(v toolkit.M, filters []*Filter) bool {
	var match bool

	for _, f := range filters {
		//toolkit.Printf("Filter:%s V:%s Has:%s Match:%s\n", toolkit.JsonString(f), toolkit.JsonString(v), v.Has(f.Field), match)
		if f.Field != "" {
			//--- if has field: $eq, $ne, $gt, $lt, $gte, $lte, $contains
			stat, val := CheckValue(v, f)
			if stat {
				match = MatchV(val, f)
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
	//toolkit.Println("MatchV: ", f.Op, v, f.Value)
	if toolkit.HasMember([]string{FilterOpEqual, FilterOpNoEqual, FilterOpGt, FilterOpGte, FilterOpLt, FilterOpLte}, f.Op) {
		return toolkit.Compare(v, f.Value, f.Op)
	} else if f.Op == FilterOpIn {
		var values []interface{}
		toolkit.FromBytes(toolkit.ToBytes(f.Value, ""), "", &values)
		return toolkit.HasMember(values, v)
	} else if f.Op == FilterOpNin {
		var values []interface{}
		toolkit.FromBytes(toolkit.ToBytes(f.Value, ""), "", &values)
		return !toolkit.HasMember(values, v)
	} else if f.Op == FilterOpContains {
		var values []interface{}
		var b bool
		toolkit.FromBytes(toolkit.ToBytes(f.Value, ""), "", &values)

		for _, val := range values {
			// value := toolkit.Sprintf(".*%s.*", val.(string))
			// b, _ = regexp.Match(value, []byte(v.(string)))
			r := regexp.MustCompile(`(?i)` + val.(string))
			b = r.Match([]byte(v.(string)))
			if b {
				return true
			}
		}
	} else if f.Op == FilterOpStartWith || f.Op == FilterOpEndWith {
		value := ""
		if f.Op == FilterOpStartWith {
			value = toolkit.Sprintf("^%s.*$", f.Value)
		} else {
			value = toolkit.Sprintf("^.*%s$", f.Value)
		}
		cond, _ := regexp.Match(value, []byte(v.(string)))
		return cond
	}
	return match
}
