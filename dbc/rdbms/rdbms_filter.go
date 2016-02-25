package rdbms

import (
	"github.com/eaciit/cast"
	"github.com/eaciit/dbox"
)

type FilterBuilder struct {
	dbox.FilterBuilder
}

func CombineIn(operator string, f *dbox.Filter) string {
	values := ""
	if operator == "LIKE " {
		for i, val := range f.Value.([]string) {
			if i == 0 {
				// values = f.Field + " " + operator + " '%" + val
				values = f.Field + " " + operator + " '%" + val + "%'"
			} else {
				// values += "%" + val
				values += "OR " + f.Field + " " + operator + " '%" + val + "%'"
			}
		}
		// values += "%'"

	} else {
		for i, val := range f.Value.([]interface{}) {
			if i == 0 {
				values = f.Field + " " + operator + " (" + StringValue(val, "non")
			} else {
				values += "," + StringValue(val, "non")
			}
		}
		values += ")"
	}
	return values
}

func (fb *FilterBuilder) BuildFilter(f *dbox.Filter) (interface{}, error) {
	fm := ""
	if f.Op == dbox.FilterOpEqual {
		fm = fm + f.Field + " = " + StringValue(f.Value, "non")
	} else if f.Op == dbox.FilterOpNoEqual {
		fm = fm + f.Field + " <>" + StringValue(f.Value, "non")
	} else if f.Op == dbox.FilterOpGt {
		fm = fm + f.Field + " > " + StringValue(f.Value, "non")
	} else if f.Op == dbox.FilterOpGte {
		fm = fm + f.Field + " >= " + StringValue(f.Value, "non")
	} else if f.Op == dbox.FilterOpLt {
		fm = fm + f.Field + " < " + StringValue(f.Value, "non")
	} else if f.Op == dbox.FilterOpLte {
		fm = fm + f.Field + " <= " + StringValue(f.Value, "non")
	} else if f.Op == dbox.FilterOpContains {
		fm = CombineIn("LIKE ", f)
	} else if f.Op == dbox.FilterOpEndWith {
		fm = fm + f.Field + " LIKE '%" + cast.ToString(f.Value) + "'"
		//fm = CombineIn("START ", f)
	} else if f.Op == dbox.FilterOpStartWith {
		fm = fm + f.Field + " LIKE '" + cast.ToString(f.Value) + "%'"
		//fm = CombineIn("START ", f)
	} else if f.Op == dbox.FilterOpIn {
		fm = CombineIn("IN", f)
	} else if f.Op == dbox.FilterOpNin {
		fm = CombineIn("NOT IN", f)
	} else if f.Op == dbox.FilterOpOr || f.Op == dbox.FilterOpAnd {
		fs := f.Value.([]*dbox.Filter)
		for _, ff := range fs {
			// nilai ff : &{name $eq Roy}
			bf, _ := fb.BuildFilter(ff)
			// nilai bf : name = 'Roy'
			if fm == "" {
				fm = "(" + cast.ToString(bf)
			} else {
				if f.Op == dbox.FilterOpOr {
					fm += " OR " + cast.ToString(bf)
				} else {
					fm += " AND " + cast.ToString(bf)
				}
			}
		}
		fm += ")"
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
