package csvs

import (
	"github.com/eaciit/cast"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"io"
	"reflect"
	//"strings"
)

type Cursor struct {
	dbox.Cursor
	indexes []int
	where   []*dbox.Filter

	q            *Query
	currentIndex int
	realIndex    int
	skip         int
	limit        int
	fields       toolkit.M
}

// dbox.Query
// 	sync.Mutex

// 	filePath string
// 	file     *os.File
// 	reader   *csv.Reader

// 	isUseHeader  bool
// 	headerColumn []headerstruct

// 	fileHasBeenOpened bool
func (c *Cursor) Close() {
}

func (c *Cursor) Count() int {
	if c.limit == 0 || c.limit > len(c.indexes) {
		c.limit = len(c.indexes)
	}

	return len(c.indexes[c.skip:c.limit])
}

func (c *Cursor) ResetFetch() error {
	var e error
	c.q.resetReader()
	c.indexes, e = c.q.generateIndex(c.where)
	c.realIndex = 0
	c.currentIndex = 0
	if e != nil {
		return errorlib.Error(packageName, modCursor, "Fetch", e.Error())
	}

	return nil
}

func (c *Cursor) Fetch(m interface{}, n int, closeWhenDone bool) error {
	if n == 0 {
		n = c.Count()
	}

	if closeWhenDone {
		defer c.Close()
	}

	if len(c.indexes) == 0 {
		return nil
	}

	var lower, upper int

	lower = c.currentIndex
	if lower < c.skip {
		lower = c.skip
	}

	upper = lower + n
	if upper > c.limit && c.limit > 0 {
		upper = c.limit
	}

	if !toolkit.IsPointer(m) {
		return errorlib.Error(packageName, modCursor, "Fetch", "Model object should be pointer")
	}

	if n != 1 && reflect.ValueOf(m).Elem().Kind() != reflect.Slice {
		return errorlib.Error(packageName, modCursor, "Fetch", "Model object should be pointer of slice")
	}

	var v reflect.Type

	if n == 1 && reflect.ValueOf(m).Elem().Kind() != reflect.Slice {
		v = reflect.TypeOf(m).Elem()
	} else {
		v = reflect.TypeOf(m).Elem().Elem()
	}

	ivs := reflect.MakeSlice(reflect.SliceOf(v), 0, 0)

	for {
		appendData := toolkit.M{}
		iv := reflect.New(v).Interface()

		datatemp, ef := c.q.reader.Read()
		c.realIndex += 1
		if c.indexes[c.currentIndex] != c.realIndex {
			continue
		}

		c.currentIndex += 1
		for i, val := range datatemp {
			if len(c.fields) == 0 || c.fields.Has("*") || c.fields.Has(c.q.headerColumn[i].name) {
				appendData[c.q.headerColumn[i].name] = val
			}
		}

		if v.Kind() == reflect.Struct {
			for i := 0; i < v.NumField(); i++ {
				if appendData.Has(v.Field(i).Name) {
					switch v.Field(i).Type.Kind() {
					case reflect.Int:
						appendData.Set(v.Field(i).Name, cast.ToInt(appendData[v.Field(i).Name], cast.RoundingAuto))
					}
				}
			}
		}

		isAppend := lower < c.currentIndex && upper >= c.currentIndex

		if (ef == io.EOF || ef == nil) && isAppend && len(appendData) > 0 {
			toolkit.Serde(appendData, iv, "json")
			ivs = reflect.Append(ivs, reflect.ValueOf(iv).Elem())
		}

		if ef != nil && ef != io.EOF {
			return errorlib.Error(packageName, modCursor, "Fetch", ef.Error())
		} else if ef == io.EOF || (ivs.Len() >= n && n > 0) {
			break
		}
	}

	// if e != nil {
	// 	return errorlib.Error(packageName, modCursor, "Fetch", e.Error())
	// }

	if n == 1 && reflect.ValueOf(m).Elem().Kind() != reflect.Slice {
		reflect.ValueOf(m).Elem().Set(ivs.Index(0))
	} else {
		reflect.ValueOf(m).Elem().Set(ivs)
	}

	return nil
}

func newCursor(q *Query) *Cursor {
	c := new(Cursor)
	c.q = q
	c.realIndex = 0
	c.currentIndex = 0
	return c
}
