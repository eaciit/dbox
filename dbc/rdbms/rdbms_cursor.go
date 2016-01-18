package rdbms

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/eaciit/dbox"
	//"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	// "reflect"
	"strconv"
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

	tableData := []toolkit.M{}
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for rows.Next() {
		for i := 0; i < count; i++ {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		entry := toolkit.M{}
		for i, col := range columns {
			var v interface{}
			val := values[i]
			// fmt.Println("Nilai val : ", val)

			b, ok := val.([]byte)
			if ok {
				v = string(b)
				integer, err := strconv.Atoi(v.(string))
				if err == nil {
					v = integer
				}
				// b, err := strconv.ParseBool(v)
				// f, err := strconv.ParseFloat(v, 64)
				// i, err := strconv.ParseInt(v, 10, 64)
				// u, err := strconv.ParseUint(v, 10, 64)
				// fmt.Println("Hasil string convert : ", b, f, i, u)

			} else {
				v = val
			}
			entry.Set(col, v)
			// e = toolkit.DecodeByte(val.([]byte), v)
			// toolkit.FromBytes(toolkit.ToBytes(val, ""), "", v)

			// entry.Set(col, v)
		}
		tableData = append(tableData, entry)
	}
	fmt.Println("Nilai table data : ", tableData)
	if e != nil {
		return e
	}
	if n == 0 {
		// *m.(*[]map[string]interface{}) = tableData
		// toolkit.Unjson(toolkit.Jsonify(tableData), m)
		e = toolkit.Serde(tableData, m, "json")
		fmt.Println("Nilai Model : ", m)
	} else {
		end := c.start + n
		if end > len(tableData) {
			e = errors.New("index out of range")
		} else {
			// *m.(*[]map[string]interface{}) = tableData[0:n]
			//toolkit.Unjson(toolkit.Jsonify(tableData[0:n]), m)
			e = toolkit.Serde(tableData[0:n], m, "json")
		}
	}
	return e
}
