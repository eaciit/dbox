package rdbms

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/eaciit/dbox"
	//"github.com/eaciit/errorlib"
	//"github.com/eaciit/toolkit"
)

const (
	modCursor = "Cursor"

	QueryResultCursor = "SQLCursor"
	QueryResultPipe   = "SQLPipe"
)

type Cursor struct {
	dbox.Cursor
	ResultType  string
	count       int
	start       int
	session     sql.DB
	QueryString string
}

func (c *Cursor) Close() {

}

func (c *Cursor) validate() error {

	return nil
}

func (c *Cursor) Count() int {
	return c.count
}

func (c *Cursor) ResetFetch() error {
	c.start = 0
	return nil
}

func (c *Cursor) Fetch(m interface{}, n int, closeWhenDone bool) error {
	fmt.Println(c.QueryString)
	rows, e := c.session.Query(c.QueryString)
	if e != nil {
		return e
	}
	defer rows.Close()
	columns, e := rows.Columns()
	if e != nil {
		return e
	}
	count := len(columns)
	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for rows.Next() {
		for i := 0; i < count; i++ {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		tableData = append(tableData, entry)
	}
	if e != nil {
		return e
	}
	if n == 0 {
		*m.(*[]map[string]interface{}) = tableData
	} else {
		end := c.start + n
		if end > len(tableData) {
			e = errors.New("index out of range")
		} else {
			*m.(*[]map[string]interface{}) = tableData[0:n]
			e = nil
		}
	}
	return e
}
