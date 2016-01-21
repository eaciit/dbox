package json

import (
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
)

const (
	modCursor = "Cursor"
)

type Cursor struct {
	dbox.Cursor
	count, lastFeteched int
	whereFields         []*dbox.Filter
	readFile            []byte
	isWhere             bool
	jsonSelect          []string
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
				last = c.count
			}
			// toolkit.Printf("first>%v last>%v lastfetched>%v count>%v\n", first, last, c.lastFeteched, c.count)
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
		var getRemField = toolkit.M{}
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
		}
	}

	e := toolkit.Serde(dataJson, m, "json")
	if e != nil {
		return errorlib.Error(packageName, modCursor, "Fetch", e.Error())
	}
	return nil
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
