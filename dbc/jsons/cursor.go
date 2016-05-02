package jsons

import (
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	// "reflect"
	//"strings"
)

type Cursor struct {
	dbox.Cursor
	indexes                                   []int
	where                                     []*dbox.Filter
	jsonSelect                                []string
	q                                         *Query
	currentIndex, skip, take, count, maxIndex int
}

func (c *Cursor) Close() {
}

func (c *Cursor) Count() int {
	return c.count
}

func (c *Cursor) ResetFetch() error {
	if c.where != nil {
		c.indexes = dbox.Find(c.q.data, c.where)
	}
	c.currentIndex = 0
	return nil
}

func (c *Cursor) Fetch(m interface{}, n int, closeWhenDone bool) error {
	var source []toolkit.M
	var lower, upper, lenData, lenIndex int
	if c.where == nil {
		lenData = len(c.q.data)
		if c.currentIndex == 0 {
			c.maxIndex = lenData
		}
	} else {
		lenIndex = len(c.indexes)
		if c.currentIndex == 0 {
			c.maxIndex = lenIndex
		}
	}

	if c.currentIndex == 0 && (c.skip > 0 || c.take > 0) { /*determine max data allowed to be fetched*/
		c.maxIndex = c.skip + c.take
	}

	lower = c.currentIndex
	upper = lower + n
	if c.skip > 0 && c.currentIndex < 1 {
		lower += c.skip
	}

	if n == 0 {
		if c.where == nil {
			upper = lenData
		} else {
			upper = lenIndex
		}

		if c.take > 0 {
			upper = lower + c.take
		}

	} else if n == 1 {
		upper = lower + 1

	} else {
		upper = lower + n
		if c.take > 0 && n > c.take {
			upper = lower + c.take
		}
	}

	if c.where == nil {
		if toolkit.SliceLen(c.q.data) > 0 {
			if lower >= lenData {
				return errorlib.Error(packageName, modCursor, "Fetch", "No more data to fetched!")
			}
			if upper >= lenData {
				upper = lenData
			}
		}
	} else {
		if toolkit.SliceLen(c.indexes) > 0 {
			if lower >= lenIndex {
				return errorlib.Error(packageName, modCursor, "Fetch", "No more data to fetched!")
			}
			if upper >= lenIndex {
				upper = lenIndex
			}
		}
	}
	if upper >= c.maxIndex {
		upper = c.maxIndex
	}

	if c.where == nil {
		source = c.q.data[lower:upper]
	} else if len(c.indexes) > 0 {
		for _, v := range c.indexes[lower:upper] {
			/*
				toolkit.Printf("Add index: %d. Source info now: %s \n", v, func() string {
					var ret []string
					for _, id := range source {
						ret = append(ret, id.Get("_id").(string))
					}
					return strings.Join(ret, ",")
				}())
			*/
			if v < len(c.q.data) {
				source = append(source, c.q.data[v])
			}
		}
	}

	if toolkit.SliceLen(c.jsonSelect) > 0 {
		source = getSelected(source, c.jsonSelect)
	}

	var e error

	if n == 1 && !toolkit.IsSlice(m) {
		if len(source) > 0 {
			e = toolkit.Serde(&source[0], m, "json")
		}
	} else {
		e = toolkit.Serde(&source, m, "json")
	}
	c.currentIndex = upper
	if e != nil {
		return errorlib.Error(packageName, modCursor, "Fetch", e.Error())
	}
	//toolkit.Printf("Data: %s\nLower, Upper = %d, %d\nSource: %s\nResult:%s\n\n", toolkit.JsonString(c.q.data), lower, upper, toolkit.JsonString(source), toolkit.JsonString(m))
	return nil
}

func getSelected(js []toolkit.M, field []string) []toolkit.M {
	var getRemField = toolkit.M{}
	for _, v := range js {
		for i, _ := range v {
			getRemField.Set(i, i)
		}

		if field[0] != "*" {
			fields := removeDuplicatesUnordered(getRemField, field)
			for _, field := range fields {
				v.Unset(field)
			}
		}
	}
	return js
}

func removeDuplicatesUnordered(elements toolkit.M, key []string) []string {
	for _, k := range key {
		elements.Unset(k)
	}

	result := []string{}
	for key, _ := range elements {
		result = append(result, key)
	}
	return result
}

func newCursor(q *Query) *Cursor {
	c := new(Cursor)
	c.q = q
	return c
}
