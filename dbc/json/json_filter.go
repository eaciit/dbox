package json

import (
	"fmt"
	"github.com/eaciit/crowd"
	"github.com/eaciit/dbox"
	. "github.com/eaciit/toolkit"
	"reflect"
	"sort"
	"strings"
	"time"
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

		for i, v := range f.Value.([]string) {
			f.Value.([]string)[i] = p.Get(v).(string)
		}
		return f
	} else {
		indirectValue := reflect.Indirect(reflect.ValueOf(f.Value))
		t := strings.ToLower(indirectValue.Kind().String())
		if !IsSlice(f.Value) {
			if t == "string" {
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
				if t == "string" {
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
			}
			return f
		}
	}
	return f
}

type SortCompare func(a, b *crowd.SortItem) bool
type changes []crowd.SortItem
type multiSorter struct {
	less    []SortCompare
	changes []crowd.SortItem
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
func (ms *multiSorter) Sort(changes []crowd.SortItem) {
	ms.changes = changes
	sort.Sort(ms)
}

func (fb *FilterBuilder) SortFetch(s []string, js []M) []M {
	var sorter []M

	var order []SortCompare
	// var Func SortCompare
	pl := make(changes, len(js))
	x := 0
	for k, v := range js {
		pl[x] = crowd.SortItem{k, v}
		x++
	}

	for _, field := range s {
		time.Sleep(400 * time.Millisecond)
		n := 1
		if field != "" {
			switch field[0] {
			case '-':
				n = -1
				field = field[1:]
			}
		}
		// Println("field", field)
		if n == 1 {
			FuncAsc := func(a, b *crowd.SortItem) bool {
				time.Sleep(400 * time.Millisecond)
				// Println("asc")
				rf := reflect.ValueOf(a.Value.(M)[field]).Kind()
				if rf == reflect.Float64 {
					ia := ToInt(a.Value.(M)[field], RoundingAuto)
					ib := ToInt(b.Value.(M)[field], RoundingAuto)
					return ia < ib
				}

				as := ToString(a.Value.(M)[field])
				bs := ToString(b.Value.(M)[field])
				return as < bs
			}
			order = append(order, FuncAsc)
		} else {
			FuncDesc := func(a, b *crowd.SortItem) bool {
				time.Sleep(400 * time.Millisecond)
				// Println("desc")
				rf := reflect.ValueOf(a.Value.(M)[field]).Kind()
				if rf == reflect.Float64 {
					ia := ToInt(a.Value.(M)[field], RoundingAuto)
					ib := ToInt(b.Value.(M)[field], RoundingAuto)
					return ia > ib
				}

				as := ToString(a.Value.(M)[field])
				bs := ToString(b.Value.(M)[field])
				return as > bs
			}
			order = append(order, FuncDesc)
		}
		// Println("here")
		// order = append(order, Func)
	}

	OrderedBy(order...).Sort(pl)
	for _, v := range pl {
		tomap, _ := ToM(v.Value)
		sorter = append(sorter, tomap)
	}

	return sorter
}
