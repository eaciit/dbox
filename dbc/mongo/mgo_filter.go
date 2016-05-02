package mongo

import (
	"fmt"
	"github.com/eaciit/dbox"
	. "github.com/eaciit/toolkit"
)

type FilterBuilder struct {
	dbox.FilterBuilder
}

func (fb *FilterBuilder) BuildFilter(f *dbox.Filter) (interface{}, error) {
	fm := M{}
	if f.Op == dbox.FilterOpEqual {
		fm.Set(f.Field, f.Value)
	} else if f.Op == dbox.FilterOpNoEqual {
		fm.Set(f.Field, M{}.Set("$ne", f.Value))
	} else if f.Op == dbox.FilterOpContains {
		fs := f.Value.([]string)
		if len(fs) > 1 {
			bfs := []interface{}{}
			for _, ff := range fs {
				pfm := M{}
				pfm.Set(f.Field, M{}.
					Set("$regex", fmt.Sprintf(".*%s.*", ff)).
					Set("$options", "i"))
				bfs = append(bfs, pfm)
			}
			fm.Set("$or", bfs)
		} else {
			fm.Set(f.Field, M{}.
				Set("$regex", fmt.Sprintf(".*%s.*", fs[0])).
				Set("$options", "i"))
		}
	} else if f.Op == dbox.FilterOpStartWith {
		fm.Set(f.Field, M{}.
			Set("$regex", fmt.Sprintf("^%s.*$", f.Value)).
			Set("$options", "i"))
	} else if f.Op == dbox.FilterOpEndWith {
		fm.Set(f.Field, M{}.
			Set("$regex", fmt.Sprintf("^.*%s$", f.Value)).
			Set("$options", "i"))
	} else if f.Op == dbox.FilterOpIn {
		fm.Set(f.Field, M{}.Set("$in", f.Value))
	} else if f.Op == dbox.FilterOpNin {
		fm.Set(f.Field, M{}.Set("$nin", f.Value))
	} else if f.Op == dbox.FilterOpGt {
		fm.Set(f.Field, M{}.Set("$gt", f.Value))
	} else if f.Op == dbox.FilterOpGte {
		fm.Set(f.Field, M{}.Set("$gte", f.Value))
	} else if f.Op == dbox.FilterOpLt {
		fm.Set(f.Field, M{}.Set("$lt", f.Value))
	} else if f.Op == dbox.FilterOpLte {
		fm.Set(f.Field, M{}.Set("$lte", f.Value))
	} else if f.Op == dbox.FilterOpOr || f.Op == dbox.FilterOpAnd {
		bfs := []interface{}{}
		fs := f.Value.([]*dbox.Filter)
		for _, ff := range fs {
			bf, eb := fb.BuildFilter(ff)
			if eb == nil {
				bfs = append(bfs, bf)
			}
		}

		fm.Set(f.Op, bfs)
	} else {
		return nil, fmt.Errorf("Filter Op %s is not defined", f.Op)
	}
	return fm, nil
}

func (fb *FilterBuilder) CombineFilters(mfs []interface{}) (interface{}, error) {
	filters := []M{}
	ret := M{}
	if len(mfs) == 0 {
		return ret, nil
	}
	if len(mfs) == 1 {
		return mfs[0].(M), nil
	}
	for _, v := range mfs {
		vm := v.(M)
		filters = append(filters, vm)
	}
	//fmt.Println(JsonString(filters))
	ret.Set("$and", filters)
	return ret, nil
}
