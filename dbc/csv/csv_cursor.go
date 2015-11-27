package csv

import (
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	_ "github.com/eaciit/toolkit"
	"io"
	"os"
	"reflect"
)

const (
	modCursor = "Cursor"
)

type WhereCond struct {
	operator  string
	condition string
}

type ConditionAttr struct {
	Find   interface{}
	Select interface{}
	Sort   []string
}

type Cursor struct {
	dbox.Cursor

	ResultType   string
	count        int
	file         *os.File
	reader       *csv.Reader
	ConditionVal ConditionAttr

	headerColumn []string
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

func (c *Cursor) Fetch(m interface{}, n int, closeWhenDone bool) (
	*dbox.DataSet, error) {
	condSelect := make(map[int]int)
	condFind := make(map[int]WhereCond)

	if closeWhenDone {
		defer c.Close()
	}

	e := c.prepIter()
	if e != nil {
		return nil, errorlib.Error(packageName, modCursor, "Fetch", e.Error())
	}

	ds := dbox.NewDataSet(m)
	lineCount := 0

	if c.ConditionVal.Select != nil {
		for i, key := range reflect.ValueOf(c.ConditionVal.Select).MapKeys() {
			temp := reflect.ValueOf(reflect.ValueOf(c.ConditionVal.Select).MapIndex(key).Interface())
			if i == 0 && key.String() == "*" {
				for n, _ := range c.headerColumn {
					condSelect[n] = 1
				}
				break
			} else {
				for n, val := range c.headerColumn {
					if val == key.String() {
						if temp.Int() == 1 {
							condSelect[n] = 1
						} else {
							condSelect[n] = 0
						}
					}
				}
			}
		}
	} else {
		for n, _ := range c.headerColumn {
			condSelect[n] = 1
		}
	}

	if c.ConditionVal.Find != nil {
		// fmt.Println("LINE 148 Ok")
		for _, key := range reflect.ValueOf(c.ConditionVal.Find).MapKeys() {
			temp := reflect.ValueOf(reflect.ValueOf(c.ConditionVal.Find).MapIndex(key).Interface())
			// fmt.Println(temp.String())
			for n, val := range c.headerColumn {
				if val == key.String() {
					condFind[n] = WhereCond{"EQ", temp.String()}
				}
			}
			break
		}
	}
	//=============================
	for {
		isAppend := true
		var dataHolder []string

		dataTemp, e := c.reader.Read()
		for i, val := range dataTemp {

			if condSelect[i] == 1 {
				dataHolder = append(dataHolder, val)
			}

			condVal, found := condFind[i]
			if found {
				if condVal.operator == "EQ" {
					if condVal.condition != val {
						isAppend = false
					}
				}
			}
		}

		if e == io.EOF {
			if isAppend && dataHolder != nil {
				ds.Data = append(ds.Data, dataHolder)
				lineCount += 1
			}
			break
		} else if e != nil {
			return ds, errorlib.Error(packageName, modCursor,
				"Fetch", e.Error())
		}
		if isAppend && dataHolder != nil {
			ds.Data = append(ds.Data, dataHolder)
			lineCount += 1
		}

		if n > 0 {
			if lineCount >= n {
				break
			}
		}
	}

	// if n == 0 {
	// 	datas := []interface{}{}
	// 	//		e = c.mgoIter.All(&datas)
	// 	if e != nil {
	// 		return ds, errorlib.Error(packageName, modCursor,
	// 			"Fetch", e.Error())
	// 	}
	// 	ds.Data = datas
	// } else if n > 0 {
	// 	fetched := 0
	// 	fetching := true
	// 	for fetching {
	// 		dataHolder := m

	// 		fetched++
	// 		if fetched == n {
	// 			fetching = false
	// 		}
	// 		ds.Data = append(ds.Data, dataHolder)
	// 		/*			if bOk := c.mgoIter.Next(&dataHolder); bOk {
	// 						ds.Data = append(ds.Data, dataHolder)
	// 						fetched++
	// 						if fetched == n {
	// 							fetching = false
	// 						}
	// 					} else {
	// 						fetching = false
	// 					}*/
	// 	}
	// }

	return ds, nil
}
