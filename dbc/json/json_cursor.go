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
	count, lastFeteched, skip, take int
	whereFields                     []*dbox.Filter
	datas                           []toolkit.M
	isWhere                         bool
	jsonSelect                      []string
}

func (c *Cursor) Close() {

}

func (c *Cursor) Count() int {
	return toolkit.SliceLen(c.datas)
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

	c.count = len(c.datas)
	c.lastFeteched = c.count
	if c.skip > 0 {
		if c.take > 0 {
			c.count = c.skip + c.take
		} else {
			first = c.skip
		}
	} else {
		c.count = c.take
	}

	if c.take > 0 {
		if c.take == c.skip {
			first = c.skip
		} else {
			first = c.count - c.take
		}
	}

	// toolkit.Printf("first = skip>%v last = take>%v lastfetched>%v count>%v\n", first, last, c.lastFeteched, c.count)
	if n == 0 {
		last = c.count
		if c.lastFeteched <= c.count || c.count == 0 {
			last = c.lastFeteched
		}

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
				c.lastFeteched = last
			}

		}

		if first > last {
			return errorlib.Error(packageName, modCursor, "Fetch", "Wrong fetched data!")
		}
	}

	if c.isWhere {
		i := dbox.Find(c.datas, c.whereFields)
		last = len(i)
		for _, index := range i[first:last] {
			dataJson = append(dataJson, c.datas[index])
		}
	} else {
		dataJson = c.datas[first:last]

	}

	if toolkit.SliceLen(c.jsonSelect) > 0 {
		dataJson = c.GetSelected(dataJson, c.jsonSelect)
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
