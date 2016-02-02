package json

import (
	"github.com/eaciit/crowd"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	// "regexp"
	// "strconv"
	// "strings"
)

const (
	modCursor = "Cursor"
)

type Cursor struct {
	dbox.Cursor
	count, lastFeteched, skip, take int
	whereFields                     []*dbox.Filter
	readFile                        []byte
	isWhere                         bool
	jsonSelect, sort                []string
	fields                          []FieldSorter
}

type FieldSorter struct {
	field string
	n     int
}

func (c *Cursor) Close() {

}

func (c *Cursor) ResetFetch() error {
	if c.lastFeteched > 0 {
		c.lastFeteched = 0
	}

	return nil
}

func (c *Cursor) Fetch(m interface{}, n int, closeWhenDone bool) error {
	if closeWhenDone {
		c.Close()
	}

	var first, last int
	dataJson := []toolkit.M{}
	var datas []toolkit.M
	toolkit.Unjson(c.readFile, &datas)

	c.count = len(datas)
	if c.skip > 0 {
		first = c.skip
	}

	if c.take > 0 {
		c.count = c.take
		// last = c.take
	}

	if n == 0 {
		last = c.count
	} else if n > 0 {
		switch {
		case c.lastFeteched == 0:
			last = n
			c.lastFeteched = n
		case n > c.lastFeteched || n < c.lastFeteched || n == c.lastFeteched:
			first = c.lastFeteched
			last = c.lastFeteched + n
			c.lastFeteched = last

			if c.lastFeteched > c.count {
				if first > c.count {
					return errorlib.Error(packageName, modCursor, "Fetch", "No more data to fetched!")
				}
				last = c.count
			}
			// toolkit.Printf("first>%v last>%v lastfetched>%v count>%v\n", first, last, c.lastFeteched, c.count)
		}

		if first > last {
			return errorlib.Error(packageName, modCursor, "Fetch", "Wrong fetched data!")
		}
	}

	if c.isWhere {
		i := dbox.Find(datas, c.whereFields)
		last = len(i)
		for _, index := range i[first:last] {
			dataJson = append(dataJson, datas[index])
		}

	} else {
		dataJson = datas[first:last]
	}

	if toolkit.SliceLen(c.jsonSelect) > 0 {
		dataJson = c.GetSelected(dataJson, c.jsonSelect)
		/*var getRemField = toolkit.M{}
		for _, v := range dataJson {
			for i, _ := range v {
				getRemField.Set(i, i)
			}

			if c.jsonSelect[0] != "*" {
				fields := c.removeDuplicatesUnordered(getRemField, c.jsonSelect)
				for _, field := range fields {
					v.Unset(field)
				}
			}
		}*/
	}

	if toolkit.SliceLen(c.sort) > 0 {
		dataJson = c.SortFetch(c.sort, dataJson)
	}

	e := toolkit.Serde(dataJson, m, "json")
	if e != nil {
		return errorlib.Error(packageName, modCursor, "Fetch", e.Error())
	}
	return nil
}

func (c *Cursor) GetSelected(js []toolkit.M, field []string) []toolkit.M {
	var getRemField = toolkit.M{}
	for _, v := range js {
		for i, _ := range v {
			getRemField.Set(i, i)
		}

		if field[0] != "*" {
			fields := c.removeDuplicatesUnordered(getRemField, field)
			for _, field := range fields {
				v.Unset(field)
			}
		}
	}
	return js
}

func (c *Cursor) removeDuplicatesUnordered(elements toolkit.M, key []string) []string {
	for _, k := range key {
		elements.Unset(k)
	}

	result := []string{}
	for key, _ := range elements {
		result = append(result, key)
	}
	return result
}

func (c *Cursor) SortFetch(s []string, js []toolkit.M) []toolkit.M {
	var i []interface{}

	for _, v := range js {
		i = append(i, v)
	}

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
		// order = append(order, toolkit.M{"field": field, "n": n})
		order = append(order, FieldSorter{field, n})
		// toolkit.Printf("field:%v n:%v\n", field, n)
	}
	// toolkit.Printf("order:%v\n", order)
	c.fields = order

	sorts := crowd.NewSortSlice(i, fsort, c.fcompare).Sort().Slice()
	var sorter []toolkit.M
	for _, v := range sorts {
		sorter = append(sorter, v.(toolkit.M))
	}
	// sort.Sort(sorter)
	return sorter
}

func fsort(so crowd.SortItem) interface{} {
	return so.Value
}

/*type lessFunc func(p1, p2 *Change) bool

type multiSorter struct {
	less []lessFunc
}

func OrderedBy(less ...lessFunc) *multiSorter {
	return &multiSorter{
		less: less,
	}
}*/

func (c *Cursor) fcompare(a, b interface{}) bool {
	var isAString, isBString, isANumber, isBNumber bool
	var ai, bi int
	var as, bs string

	for _, v := range c.fields {
		as = toolkit.ToString(a.(toolkit.M)[v.field])
		bs = toolkit.ToString(b.(toolkit.M)[v.field])

		if ia := toolkit.ToInt(as, toolkit.RoundingAuto); ia > 0 {
			// ai = ia
			isANumber = true
		} else {
			isAString = true
		}

		if ib := toolkit.ToInt(bs, toolkit.RoundingAuto); ib > 0 {
			// bi = ib
			isBNumber = true
		} else {
			isBString = true
		}

		if v.n == -1 {
			if isANumber && isBNumber {
				return ai >= bi
			} else if isAString && isBString {
				return as >= bs
			}
		}

		if isANumber && isBNumber {
			return ai < bi
		} else if isAString && isBString {
			return as < bs
		}

		// return ai >= bi && as < bs
	}

	return false
}
