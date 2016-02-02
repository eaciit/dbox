package json

import (
	"fmt"
	"github.com/eaciit/crowd"
	"github.com/eaciit/dbox"
	. "github.com/eaciit/toolkit"
	"reflect"
	// "sort"
	"strings"
)

type FilterBuilder struct {
	dbox.FilterBuilder
	fields []FieldSorter
}

type Sorter struct {
	f      []FieldSorter
	sorter []M
}

type FieldSorter struct {
	field string
	n     int
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
			splitString := strings.Split(v, "@")
			valueToString := ToString(splitString[1])
			f.Value.([]string)[i] = p.Get(valueToString).(string)
		}
		return f
	} else {
		if !IsSlice(f.Value) {
			fTostring := ToString(f.Value)
			foundSubstring := strings.Index(fTostring, "@")
			if foundSubstring != 0 {
				return f
			}

			if strings.Contains(fTostring, "@") {
				splitParm := strings.Split(fTostring, "@")
				f.Value = p.Get(splitParm[1])
				return f
			}
		} else {
			var splitValue []string

			for i, v := range f.Value.([]interface{}) {
				vToString := ToString(v)
				foundSubstring := strings.Index(vToString, "@")
				if foundSubstring != 0 {
					return f
				}
				if strings.Contains(vToString, "@") {
					splitValue = strings.Split(vToString, "@")
				}
				switch Kind(v) {
				case reflect.String:
					stringValue := ToString(p.Get(splitValue[1]))
					f.Value.([]interface{})[i] = stringValue
				case reflect.Int:
					stringValue := ToInt(p.Get(splitValue[1]), ".")
					f.Value.([]interface{})[i] = stringValue
				case reflect.Bool:
					f.Value.([]interface{})[i] = p.Get(splitValue[1]).(bool)
				}
			}
			return f
		}
	}
	return f
}

/*type SortCompare func(a, b interface{}) bool
type Change struct {
	Age      int    `json:"Age"`
	Enable   bool   `json:"Enable"`
	FullName string `json:"FullName"`
	id       string `json:"_id"`
}
type multiSorter struct {
	less    []SortCompare
	changes []Change
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
func (ms *multiSorter) Sort(changes []Change) {
	ms.changes = changes
	sort.Sort(ms)
}*/

func (sr *Sorter) SortFetch(s []string, js []M) []M {
	var i []interface{}
	var sorter []M

	var order []FieldSorter //[]toolkit.M
	for _, field := range s {
		n := 1
		if field != "" {
			switch field[0] {
			case '+':
				field = field[1:]
			case '-':
				n = -1
				field = field[1:]
			}
		}
		order = append(order, FieldSorter{field, n})
	}

	for _, v := range js {
		i = append(i, v)
	}

	/*age := func(a, b interface{}) bool {
		return a.(*Change).Age < b.(*Change).Age
	}
	fullname := func(a, b interface{}) bool {
		return a.(*Change).FullName < b.(*Change).FullName
	}

	jstring := JsonString(js)
	changes := []Change{}
	Unjson([]byte(jstring), &changes)
	OrderedBy(age, fullname).Sort(changes)
	Printf("a:%v\n", JsonString(changes))*/

	sr.f = order
	val := crowd.NewSortSlice(i, fsort, sr.fcompare).Sort().Slice()

	for _, v := range val {
		sorter = append(sorter, v.(M))
	}

	return sorter
}

func fsort(so crowd.SortItem) interface{} {
	return so.Value
}

func (sr *Sorter) fcompare(a, b interface{}) bool {
	for _, field := range sr.f {
		ia := ToInt(a.(M)[field.field], RoundingAuto)
		ib := ToInt(b.(M)[field.field], RoundingAuto)
		if ia > 0 && ib > 0 {
			if field.n == -1 {
				return ia > ib
			}
			return ia < ib
		}

		as := ToString(a.(M)[field.field])
		bs := ToString(b.(M)[field.field])
		if as != "" && bs != "" {
			if field.n == -1 {
				return as > bs
			}
			return as < bs
		}
	}
	return false
}
