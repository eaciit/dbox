package jsons

import (
	"github.com/eaciit/crowd"
	"github.com/eaciit/dbox"
	. "github.com/eaciit/toolkit"
	"reflect"
	"sort"
	"time"
)

type FilterBuilder struct {
	dbox.FilterBuilder
}

type SortCompare func(a, b *crowd.SortItem) bool
type changes []crowd.SortItem
type multiSorter struct {
	less    []SortCompare
	changes []crowd.SortItem
}

func (fb *FilterBuilder) BuildFilter(f *dbox.Filter) (interface{}, error) {
	return nil, nil
}

func (fb *FilterBuilder) CombineFilters(fs []interface{}) (interface{}, error) {
	return nil, nil
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
	var Func SortCompare
	pl := make(changes, len(js))
	x := 0
	for k, v := range js {
		pl[x] = crowd.SortItem{k, v}
		x++
	}

	for _, field := range s {
		time.Sleep(1 * time.Second)
		n := 1
		if field != "" {
			switch field[0] {
			case '-':
				n = -1
				field = field[1:]
			}
		}

		if n == 1 {
			Func = func(a, b *crowd.SortItem) bool {
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
		} else {
			Func = func(a, b *crowd.SortItem) bool {
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
