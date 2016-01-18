package rdbms

import (
	"fmt"
	"github.com/eaciit/cast"
	"github.com/eaciit/dbox"
	"time"
)

type FilterBuilder struct {
	dbox.FilterBuilder
	//Query
	//rdbms.Connection
	//Connection
}

func StringValues(v interface{}) string {
	var ret string
	switch v.(type) {
	case string:
		ret = fmt.Sprintf("%s", "'"+v.(string)+"'")
	case time.Time:
		t := v.(time.Time).UTC()
		ret = "'" + t.Format("2006-01-02 15:04:05") + "'"
		// if strings.Contains(db, "oracle") {
		// 	ret = "to_date('" + t.Format("2006-01-02 15:04:05") + "','yyyy-mm-dd hh24:mi:ss')"
		// } else {
		// 	ret = "'" + t.Format("2006-01-02 15:04:05") + "'"
		// }
	case int, int32, int64, uint, uint32, uint64:
		ret = fmt.Sprintf("%d", v.(int))
	case nil:
		ret = ""
	default:
		ret = fmt.Sprintf("%v", v)
		//-- do nothing
	}
	return ret
}

func CombineIn(operator string, f *dbox.Filter) string {
	values := ""
	for i, val := range f.Value.([]interface{}) {
		if i == 0 {
			values = f.Field + " " + operator + " (" + StringValues(val)
		} else {
			values += "," + StringValues(val)
		}
	}
	values += ")"
	return values
}

func (fb *FilterBuilder) BuildFilter(f *dbox.Filter) (interface{}, error) {
	fm := ""
	// vals := ""
	//drivername :=  dbox.Connection
	// drivername :=  new(Connection)
	//drivername :=  fb.GetDriver()
	// drivername := fb.Connection().(*Connection).Drivername
	// fmt.Println("drivernamenya adalah : ", drivername)
	if f.Op == dbox.FilterOpEqual {
		fm = fm + f.Field + "= '" + cast.ToString(f.Value) + "'"
	} else if f.Op == dbox.FilterOpNoEqual {
		fm = fm + f.Field + "<>'" + cast.ToString(f.Value) + "'"
	} else if f.Op == dbox.FilterOpGt {
		fm = fm + f.Field + " > '" + cast.ToString(f.Value) + "'"
	} else if f.Op == dbox.FilterOpGte {
		fm = fm + f.Field + " >= '" + cast.ToString(f.Value) + "'"
	} else if f.Op == dbox.FilterOpLt {
		fm = fm + f.Field + " < '" + cast.ToString(f.Value) + "'"
	} else if f.Op == dbox.FilterOpLte {
		fm = fm + f.Field + " <= '" + cast.ToString(f.Value) + "'"
	} else if f.Op == dbox.FilterOpIn {
		fm = CombineIn("IN", f)
	} else if f.Op == dbox.FilterOpNin {
		fm = CombineIn("NOT IN", f)
	} else if f.Op == dbox.FilterOpContains {
		fm = CombineIn("NOT IN", f)
	} else if f.Op == dbox.FilterOpOr || f.Op == dbox.FilterOpAnd {
		// bfs := []interface{}{}
		fs := f.Value.([]*dbox.Filter)
		for _, ff := range fs {
			bf, _ := fb.BuildFilter(ff)
			// if eb == nil {
			// 	bfs = append(bfs, bf)
			// }
			if fm == "" {
				fm = cast.ToString(bf)
			} else {
				if f.Op == dbox.FilterOpOr {
					fm = fm + " OR " + cast.ToString(bf)
				} else {
					fm = fm + " AND " + cast.ToString(bf)
				}
			}
		}
		//fm.Set(f.Op, bfs)
	} else {
		//return nil, fmt.Errorf("Filter Op %s is not defined", f.Op)
	}

	return fm, nil
}

func (fb *FilterBuilder) CombineFilters(mfs []interface{}) (interface{}, error) {
	ret := ""
	if len(mfs) == 0 {
		return ret, nil
	}
	if len(mfs) == 1 {
		return mfs[0].(string), nil
	}
	for _, v := range mfs {
		vm := v.(string)
		if ret == "" {
			ret = vm
		} else {
			ret = ret + " AND " + vm
		}
	}
	return ret, nil
}
