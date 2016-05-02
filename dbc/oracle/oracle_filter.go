package oracle

import (
	"github.com/eaciit/cast"
	"github.com/eaciit/dbox"
	"github.com/eaciit/dbox/dbc/rdbms"
)

type FilterBuilder struct {
	dbox.FilterBuilder
}

func CombineIn(operator string, f *dbox.Filter) string {
	values := ""
	for i, val := range f.Value.([]interface{}) {
		if i == 0 {
			values = f.Field + " " + operator + " (" + rdbms.StringValue(val, "oracle")
		} else {
			values += "," + rdbms.StringValue(val, "oracle")
		}
	}
	values += ")"
	return values
}

func (fb *FilterBuilder) BuildFilter(f *dbox.Filter) (interface{}, error) {
	fm := ""
	if f.Op == dbox.FilterOpEqual {
		fm = fm + f.Field + " = " + rdbms.StringValue(f.Value, "oracle") + ""
	} else if f.Op == dbox.FilterOpNoEqual {
		fm = fm + f.Field + " <> " + rdbms.StringValue(f.Value, "oracle") + ""
	} else if f.Op == dbox.FilterOpGt {
		fm = fm + f.Field + " > " + rdbms.StringValue(f.Value, "oracle") + ""
	} else if f.Op == dbox.FilterOpGte {
		fm = fm + f.Field + " >= " + rdbms.StringValue(f.Value, "oracle") + ""
	} else if f.Op == dbox.FilterOpLt {
		fm = fm + f.Field + " < " + rdbms.StringValue(f.Value, "oracle") + ""
	} else if f.Op == dbox.FilterOpLte {
		fm = fm + f.Field + " <= " + rdbms.StringValue(f.Value, "oracle") + ""
	} else if f.Op == dbox.FilterOpIn {
		fm = CombineIn("IN", f)
	} else if f.Op == dbox.FilterOpNin {
		fm = CombineIn("NOT IN", f)
	} else if f.Op == dbox.FilterOpContains {
		fm = CombineIn("NOT IN", f)
	} else if f.Op == dbox.FilterOpOr || f.Op == dbox.FilterOpAnd {
		// f
		fs := f.Value.([]*dbox.Filter)
		for _, ff := range fs {
			bf, _ := fb.BuildFilter(ff)
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
