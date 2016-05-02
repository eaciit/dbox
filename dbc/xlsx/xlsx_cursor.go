package xlsx

import (
	// "encoding/json"
	"errors"
	"fmt"
	"github.com/eaciit/dbox"
	// _ "github.com/eaciit/dbox/dbc/xlsx"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"github.com/tealeg/xlsx"
	// "io"
	// "os"
	"reflect"
	// "strconv"
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

	sheetname string
	fetchRow  int
	count     int
	// file         *os.File
	reader       *xlsx.File
	ConditionVal QueryCondition

	headerColumn []headerstruct
	rowstart     int
	colstart     int
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
	// fmt.Println("LINE-60", c.sheetname)
	if len(c.ConditionVal.Find) == 0 {
		return c.reader.Sheet[c.sheetname].MaxRow
	} else {
		x := 0
		for _, row := range c.reader.Sheet[c.sheetname].Rows {
			isAppend := true
			recData := toolkit.M{}
			for i, cell := range row.Cells {
				recData.Set(c.headerColumn[i].name, cell.Value)
			}

			isAppend = c.ConditionVal.getCondition(recData)
			// fmt.Printf("%v - %v - %#v \n", isAppend, recData["1"], c.ConditionVal.Find)
			if isAppend {
				x += 1
			}
		}
		// fmt.Println("Masuk Condition, Jumlah data : ", x)
		return x
	}
	return 0
}

func (c *Cursor) ResetFetch() error {
	c.fetchRow = 0

	e := c.prepIter()
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
	// ci := c.aa

	if closeWhenDone {
		defer c.Close()
	}

	// if !toolkit.IsPointer(m) {
	// 	return errorlib.Error(packageName, modCursor, "Fetch", "Model object should be pointer")
	// }
	if n != 1 && reflect.ValueOf(m).Elem().Kind() != reflect.Slice {
		return errorlib.Error(packageName, modCursor, "Fetch", "Model object should be pointer of slice")
	}

	e := c.prepIter()
	if e != nil {
		return errorlib.Error(packageName, modCursor, "Fetch", e.Error())
	}

	datas := []toolkit.M{}
	// lineCount := 0
	//=============================
	maxGetData := c.count
	if n > 0 {
		maxGetData = c.fetchRow + n
	}

	linecount := 0

	for i, row := range c.reader.Sheet[c.sheetname].Rows {
		isAppend := true
		recData := toolkit.M{}
		appendData := toolkit.M{}

		for i, cell := range row.Cells {
			if i < len(c.headerColumn) {
				recData.Set(c.headerColumn[i].name, cell.Value)

				if len(c.ConditionVal.Select) == 0 || c.ConditionVal.Select.Get("*", 0).(int) == 1 {
					appendData.Set(c.headerColumn[i].name, cell.Value)
				} else {
					if c.ConditionVal.Select.Get(c.headerColumn[i].name, 0).(int) == 1 {
						appendData.Set(c.headerColumn[i].name, cell.Value)
					}
				}
			}
		}

		//

		isAppend = c.ConditionVal.getCondition(recData)

		if c.fetchRow < c.ConditionVal.skip || (c.fetchRow > (c.ConditionVal.skip+c.ConditionVal.limit) && c.ConditionVal.limit > 0) {
			isAppend = false
		}

		aa := c.rowstart

		if i <= aa {
			isAppend = false
			linecount += 1
		}

		if isAppend && len(appendData) > 0 {
			linecount += 1
			if linecount > c.fetchRow {
				datas = append(datas, appendData)
				c.fetchRow += 1
			}
		}

		// fmt.Println("max :",maxGetData)
		// fmt.Println("fetch :",c.fetchRow)

		if c.fetchRow >= maxGetData {
			break
		}
	}

	e = toolkit.Unjson(toolkit.Jsonify(datas), m)
	if e != nil {
		return errorlib.Error(packageName, modCursor, "Fetch", e.Error())
	}

	return nil
}
