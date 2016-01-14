package csv

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"io"
	"os"
	"reflect"
)

const (
	modCursor = "Cursor"
)

// type ConditionAttr struct {
// 	Find   toolkit.M
// 	Select toolkit.M
// 	Sort   []string
// 	skip   int
// 	limit  int
// }

type Cursor struct {
	dbox.Cursor

	ResultType   string
	count        int
	file         *os.File
	reader       *csv.Reader
	ConditionVal QueryCondition

	headerColumn []headerstruct
}

func (c *Cursor) Close() {
}

func (c *Cursor) validate() error {
	if c.reader == nil {
		return errors.New(fmt.Sprintf("Reader is nil"))
	}

	return nil
}

func (c *Cursor) prepIter() error {
	e := c.validate()
	if e != nil {
		return e
	}
	return nil
}

func (c *Cursor) Count() int {
	return c.count
}

func (c *Cursor) ResetFetch() error {
	var e error
	c.Connection().(*Connection).Close()
	e = c.Connection().(*Connection).Connect()

	if e != nil {
		return errorlib.Error(packageName, modCursor, "Restart Connection", e.Error())
	}

	c.headerColumn = c.Connection().(*Connection).headerColumn
	c.file = c.Connection().(*Connection).file
	c.reader = c.Connection().(*Connection).reader

	e = c.prepIter()
	if e != nil {
		return errorlib.Error(packageName, modCursor, "ResetFetch", e.Error())
	}

	// c.PrepareCursor()
	// if e != nil {
	// 	return errorlib.Error(packageName, modCursor, "Prepare Cursor", e.Error())
	// }

	return nil
}

// func (c *Cursor) PrepareCursor() error {
// 	var e error

// 	c.headerColumn, e = c.reader.Read()
// 	if e != nil {
// 		return e
// 	}
// 	return nil
// }

func (c *Cursor) Fetch(m interface{}, n int, closeWhenDone bool) error {

	if closeWhenDone {
		defer c.Close()
	}

	e := c.prepIter()
	if e != nil {
		return errorlib.Error(packageName, modCursor, "Fetch", e.Error())
	}

	// if !toolkit.IsPointer(m) {
	// 	return errorlib.Error(packageName, modCursor, "Fetch", "Model object should be pointer")
	// }
	if n != 1 && reflect.ValueOf(m).Elem().Kind() != reflect.Slice {
		return errorlib.Error(packageName, modCursor, "Fetch", "Model object should be pointer of slice")
	}
	// fmt.Println("LINE 112 : ", reflect.ValueOf(m).Elem().Kind())
	// ds := dbox.NewDataSet(m)
	// rm := reflect.ValueOf(m)
	// // erm := rm.Elem()
	// fmt.Println(rm)
	// var v reflect.Type

	// if n == 1 {
	// 	v = reflect.TypeOf(m).Elem()
	// } else {
	// 	v = reflect.TypeOf(m).Elem().Elem()
	// }

	// vdatas := reflect.MakeSlice(reflect.SliceOf(v), 0, n)
	// fmt.Println("LINE 122 : ", reflect.TypeOf(vdatas))
	datas := []toolkit.M{}
	lineCount := 0

	//=============================
	for {
		isAppend := true
		c.count += 1
		recData := toolkit.M{}
		appendData := toolkit.M{}

		dataTemp, e := c.reader.Read()

		for i, val := range dataTemp {
			recData[c.headerColumn[i].name] = val

			if c.ConditionVal.Select == nil || c.ConditionVal.Select.Get("*", 0).(int) == 1 {
				appendData[c.headerColumn[i].name] = val
			} else {
				if c.ConditionVal.Select.Get(c.headerColumn[i].name, 0).(int) == 1 {
					appendData[c.headerColumn[i].name] = val
				}
			}
		}

		isAppend = c.ConditionVal.getCondition(recData)

		if c.count < c.ConditionVal.skip || (c.count > (c.ConditionVal.skip+c.ConditionVal.limit) && c.ConditionVal.limit > 0) {
			isAppend = false
		}

		if e == io.EOF {
			if isAppend && len(appendData) > 0 {
				datas = append(datas, appendData)
				lineCount += 1
			}
			break
		} else if e != nil {
			return errorlib.Error(packageName, modCursor,
				"Fetch", e.Error())
		}
		if isAppend && len(appendData) > 0 {
			datas = append(datas, appendData)
			lineCount += 1
		}

		if n > 0 {
			if lineCount >= n {
				break
			}
		}
	}

	bs, _ := json.Marshal(datas)
	_ = json.Unmarshal(bs, m)

	return nil
}
