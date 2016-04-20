package json

import (
	"fmt"
	"github.com/eaciit/crowd"
	"github.com/eaciit/dbox"
	. "github.com/eaciit/toolkit"
	"reflect"
	"sort"
	"strings"
	// "time"
)

type FilterBuilder struct {
	dbox.FilterBuilder
	// fields []FieldSorter
}

func (fb *FilterBuilder) BuildFilter(f *dbox.Filter) (interface{}, error) {
	fm := M{}
	if f.Op == dbox.FilterOpEqual {
		fm.Set(f.Field, f.Value)
	} else if f.Op == dbox.FilterOpNoEqual {
		fm.Set(f.Field, M{}.Set("$ne", f.Value))
	} else if f.Op == dbox.FilterOpContains {
		fm.Set(f.Field, M{}.
			Set("$regex", fmt.Sprintf(".*%s.*", f.Value)).
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
	ret.Set("$and", filters)
	return ret, nil
}

func (fb *FilterBuilder) CheckFilter(f *dbox.Filter, p M) *dbox.Filter {
	if f.Op == "$or" || f.Op == "$and" {
		fs := f.Value.([]*dbox.Filter)
		for i, ff := range fs {
			bf := fb.CheckFilter(ff, p)
			fs[i] = bf
		}
		return f
	} else if f.Op == "$contains" {
		// Println(f.Value)
		for i, v := range f.Value.([]string) {
			if p != nil {
				f.Value.([]string)[i] = p.Get(v).(string)
			} else {
				f.Value.([]string)[i] = v
			}

		}
		return f
	} else {
		if !IsSlice(f.Value) {
			if strings.ToLower(Kind(f.Value).String()) == "string" {
				foundSubstring := strings.Index(f.Value.(string), "@")
				if foundSubstring != 0 {
					return f
				}

				if strings.Contains(f.Value.(string), "@") {
					f.Value = p.Get(f.Value.(string))
					return f
				}
			}
		} else {
			for i, v := range f.Value.([]interface{}) {
				foundSubstring := strings.Index(v.(string), "@")
				if foundSubstring != 0 {
					return f
				}

				switch Kind(v) {
				case reflect.String:
					stringValue := p.Get(v.(string))
					f.Value.([]interface{})[i] = stringValue
				case reflect.Int:
					stringValue := ToInt(p.Get(v.(string)), ".")
					f.Value.([]interface{})[i] = stringValue
				case reflect.Bool:
					f.Value.([]interface{})[i] = p.Get(v.(string)).(bool)
				}
			}
			return f
		}
	}
	return f
}

type SortCompare func(a, b *crowd.KV) bool
type changes []crowd.KV
type multiSorter struct {
	less    []SortCompare
	changes []crowd.KV
}

func (ms *multiSorter) Len() int {
	return len(ms.changes)
}

func (ms *multiSorter) Swap(i, j int) {
	ms.changes[i], ms.changes[j] = ms.changes[j], ms.changes[i]
}
func (ms *multiSorter) Less(i, j int) bool {
	p, q := &ms.changes[i], &ms.changes[j]
	var k int
	for k = 0; k < len(ms.less)-1; k++ {
		less := ms.less[k]
		switch {
		case less(p, q):
			return true
		case less(q, p):
			return false
		}
	}
	return ms.less[k](p, q)
}
func OrderedBy(less ...SortCompare) *multiSorter {
	return &multiSorter{
		less: less,
	}
}
func (ms *multiSorter) Sort(changes []crowd.KV) {
	ms.changes = changes
	sort.Sort(ms)
}

func (fb *FilterBuilder) SortFetch(s []string, js []M) []M {
	var sorter []M

	var order []SortCompare
	var /*FuncAsc, FuncDesc*/ Func SortCompare
	pl := make(changes, len(js))
	x := 0
	for k, v := range js {
		pl[x] = crowd.KV{k, v}
		x++
	}

	for _, field := range s {
		n := 1
		if field != "" {
			switch field[0] {
			case '-':
				n = -1
				field = field[1:]
			}
		}

		Func = func(a, b *crowd.KV) bool {
			rf := reflect.ValueOf(a.Value.(M)[field]).Kind()
			if rf == reflect.Float64 {
				// ia := ToInt(a.Value.(M)[field], RoundingAuto)
				// ib := ToInt(b.Value.(M)[field], RoundingAuto)
				ia := ToFloat64(a.Value.(M)[field], 2, RoundingAuto)
				ib := ToFloat64(b.Value.(M)[field], 2, RoundingAuto)
				if n == 1 {
					return ia < ib
				} else {
					return ia > ib
				}

			}

			as := ToString(a.Value.(M)[field])
			bs := ToString(b.Value.(M)[field])
			if n == 1 {
				return as < bs
			} else {
				return as > bs
			}
		}
		order = append(order, Func)
	}

	OrderedBy(order...).Sort(pl)
	for _, v := range pl {
		tomap, _ := ToM(v.Value)
		sorter = append(sorter, tomap)
	}

	return sorter
}
