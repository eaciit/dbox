package csv

import (
	"encoding/csv"
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

// type WhereCond struct {
// 	operator  string
// 	condition string
// }

type ConditionAttr struct {
	Find   toolkit.M
	Select toolkit.M
	Sort   []string
	skip   int
	limit  int
}

type Cursor struct {
	dbox.Cursor

	ResultType   string
	count        int
	file         *os.File
	reader       *csv.Reader
	ConditionVal ConditionAttr

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

func (c *Cursor) Fetch(m interface{}, n int, closeWhenDone bool) (
	*dbox.DataSet, error) {
	// condSelect := make(map[int]int)
	// condFind := make(map[int]WhereCond)

	if closeWhenDone {
		defer c.Close()
	}

	e := c.prepIter()
	if e != nil {
		return nil, errorlib.Error(packageName, modCursor, "Fetch", e.Error())
	}

	ds := dbox.NewDataSet(m)
	lineCount := 0

	// if c.ConditionVal.Select != nil {
	// 	for i, key := range reflect.ValueOf(c.ConditionVal.Select).MapKeys() {
	// 		temp := reflect.ValueOf(reflect.ValueOf(c.ConditionVal.Select).MapIndex(key).Interface())
	// 		if i == 0 && key.String() == "*" {
	// 			for n, _ := range c.headerColumn {
	// 				condSelect[n] = 1
	// 			}
	// 			break
	// 		} else {
	// 			for n, val := range c.headerColumn {
	// 				if val.name == key.String() {
	// 					if temp.Int() == 1 {
	// 						condSelect[n] = 1
	// 					} else {
	// 						condSelect[n] = 0
	// 					}
	// 				}
	// 			}
	// 		}
	// 	}
	// } else {
	// 	for n, _ := range c.headerColumn {
	// 		condSelect[n] = 1
	// 	}
	// }
	//complex where query
	// if c.ConditionVal.Find != nil {
	// 	for _, key := range reflect.ValueOf(c.ConditionVal.Find).MapKeys() {
	// 		temp := reflect.ValueOf(reflect.ValueOf(c.ConditionVal.Find).MapIndex(key).Interface())
	// 		for n, val := range c.headerColumn {
	// 			if val.name == key.String() {
	// 				condFind[n] = WhereCond{"EQ", temp.String()}
	// 			}
	// 		}
	// 		break
	// 	}
	// }

	//=============================
	fmt.Println(c.ConditionVal.Find)
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

		isAppend = foundCondition(recData, c.ConditionVal.Find)
		// for i, val := range dataTemp {

		// 	if condSelect[i] == 1 {
		// 		appendData[c.headerColumn[i].name] = val
		// 	}

		// 	condVal, found := condFind[i]
		// 	if found {
		// 		if condVal.operator == "EQ" {
		// 			if condVal.condition != val {
		// 				isAppend = false
		// 				c.count -= 1
		// 			}
		// 		}
		// 	}
		// }

		if c.count < c.ConditionVal.skip || (c.count > (c.ConditionVal.skip+c.ConditionVal.limit) && c.ConditionVal.limit > 0) {
			isAppend = false
		}

		if e == io.EOF {
			if isAppend && len(appendData) > 0 {
				ds.Data = append(ds.Data, appendData)
				lineCount += 1
			}
			break
		} else if e != nil {
			return ds, errorlib.Error(packageName, modCursor,
				"Fetch", e.Error())
		}
		if isAppend && len(appendData) > 0 {
			ds.Data = append(ds.Data, appendData)
			lineCount += 1
		}

		if n > 0 {
			if lineCount >= n {
				break
			}
		}
	}
	return ds, nil
}

func toSliceM(v interface{}) []toolkit.M {
	slicetVal := []toolkit.M{}
	// for i, val := range v {
	fmt.Println("Line 229 : ", reflect.ValueOf(v))
	// }
	return slicetVal
}

func foundCondition(dataCheck toolkit.M, cond toolkit.M) bool {
	resBool := true

	for key, val := range cond {
		if key == "$and" || key == "$or" {
			for i, sVal := range val.([]interface{}) {
				rVal := sVal.(map[string]interface{})
				mVal := toolkit.M{}
				for rKey, mapVal := range rVal {
					mVal.Set(rKey, mapVal)
				}

				xResBool := foundCondition(dataCheck, mVal)
				if key == "$and" {
					resBool = resBool && xResBool
				} else {
					if i == 0 {
						resBool = xResBool
					} else {
						resBool = resBool || xResBool
					}
				}
			}
		} else if val != dataCheck.Get(key, "").(string) {
			resBool = false
		}
	}

	return resBool
}
