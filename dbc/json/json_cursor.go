package json

import (
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
	count, lastFetched, skip, take, maxIndex int
	indexes                                  []int
	whereFields                              []*dbox.Filter
	datas                                    []toolkit.M
	isWhere                                  bool
	jsonSelect                               []string
}

func (c *Cursor) Close() {

}

func (c *Cursor) Count() int {
	return c.count
}

func (c *Cursor) ResetFetch() error {
	if c.lastFetched > 0 {
		c.lastFetched = 0
	}

	return nil
}

func (c *Cursor) Fetch(m interface{}, n int, closeWhenDone bool) error {
	if closeWhenDone {
		c.Close()
	}

	var source []toolkit.M
	var lower, upper, lenData, lenIndex int
	if !c.isWhere {
		lenData = len(c.datas)
		if c.lastFetched == 0 {
			c.maxIndex = lenData
		}
	} else {
		lenIndex = len(c.indexes)
		if c.lastFetched == 0 {
			c.maxIndex = lenIndex
		}
	}

	if c.lastFetched == 0 && (c.skip > 0 || c.take > 0) { /*determine max data allowed to be fetched*/
		c.maxIndex = c.skip + c.take
	}

	lower = c.lastFetched
	upper = lower + n
	if c.skip > 0 && c.lastFetched < 1 {
		lower += c.skip
	}

	if n == 0 {
		if !c.isWhere {
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

	if !c.isWhere {
		if toolkit.SliceLen(c.datas) > 0 {
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

	if !c.isWhere {
		source = c.datas[lower:upper]
	} else {
		for _, v := range c.indexes[lower:upper] {
			if v < len(c.datas) {
				source = append(source, c.datas[v])
			}
		}
	}

	if toolkit.SliceLen(c.jsonSelect) > 0 {
		source = c.GetSelected(source, c.jsonSelect)
	}

	var e error
	e = toolkit.Serde(&source, m, "json")

	c.lastFetched = upper
	if e != nil {
		return errorlib.Error(packageName, modCursor, "Fetch", e.Error())
	}

	// var first, last int
	// dataJson := []toolkit.M{}

	// c.count = len(c.datas)
	// c.lastFetched = c.count
	// if c.skip > 0 {
	// 	if c.take > 0 {
	// 		c.count = c.skip + c.take
	// 	} else {
	// 		first = c.skip
	// 	}
	// } else {
	// 	c.count = c.take
	// }

	// if c.take > 0 {
	// 	if c.take == c.skip {
	// 		first = c.skip
	// 	} else {
	// 		first = c.count - c.take
	// 	}
	// }

	// // toolkit.Printf("first = skip>%v last = take>%v lastfetched>%v count>%v\n", first, last, c.lastFetched, c.count)
	// if n == 0 {
	// 	last = c.count
	// 	if c.lastFetched <= c.count || c.count == 0 {
	// 		last = c.lastFetched
	// 	}

	// } else if n > 0 {
	// 	switch {
	// 	case c.lastFetched == 0:
	// 		last = n
	// 		c.lastFetched = n
	// 	case n > c.lastFetched || n < c.lastFetched || n == c.lastFetched:
	// 		first = c.lastFetched
	// 		last = c.lastFetched + n
	// 		c.lastFetched = last

	// 		if c.lastFetched > c.count {
	// 			if first > c.count {
	// 				return errorlib.Error(packageName, modCursor, "Fetch", "No more data to fetched!")
	// 			}
	// 			last = c.count
	// 			c.lastFetched = last
	// 		}

	// 	}

	// 	if first > last {
	// 		return errorlib.Error(packageName, modCursor, "Fetch", "Wrong fetched data!")
	// 	}
	// }

	// if c.isWhere {
	// 	i := dbox.Find(c.datas, c.whereFields)
	// 	c.lastFetched = len(i)
	// 	if c.lastFetched < c.count || c.count == 0 {
	// 		last = c.lastFetched
	// 	}
	// 	for _, index := range i[first:last] {
	// 		dataJson = append(dataJson, c.datas[index])
	// 	}
	// } else {
	// 	dataJson = c.datas[first:last]

	// }

	// if toolkit.SliceLen(c.jsonSelect) > 0 {
	// 	dataJson = c.GetSelected(dataJson, c.jsonSelect)
	// }

	// e := toolkit.Serde(dataJson, m, "json")
	// if e != nil {
	// 	return errorlib.Error(packageName, modCursor, "Fetch", e.Error())
	// }
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
