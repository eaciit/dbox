package rdbms

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/eaciit/dbox"
	//"github.com/eaciit/errorlib"
	"github.com/eaciit/cast"
	"github.com/eaciit/toolkit"
	"reflect"
	"strings"
	// "strconv"
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

	var valueType reflect.Type

	if n == 1 {
		valueType = reflect.TypeOf(m).Elem()
	} else {
		valueType = reflect.TypeOf(m).Elem().Elem()
	}

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
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry.Set(strings.ToLower(col), v)
		}

		if valueType.Kind() == reflect.Struct {
			for i := 0; i < valueType.NumField(); i++ {
				namaField := strings.ToLower(valueType.Field(i).Name)
				dataType := strings.ToLower(valueType.Field(i).Type.String())

				if entry.Has(namaField) {
					if strings.Contains(dataType, "int") {
						entry.Set(namaField,
							cast.ToInt(entry[namaField], cast.RoundingAuto))
					} else if strings.Contains(dataType, "time.time") {
						entry.Set(namaField,
							cast.String2Date(cast.ToString(entry[namaField]), "2006-01-02 15:04:05"))
					}
				}
			}
		}

		tableData = append(tableData, entry)
	}

	// fmt.Println("Nilai table data : ", tableData)
	if e != nil {
		return e
	}
	if n == 0 {
		e = toolkit.Serde(tableData, m, "json")
		// fmt.Println("Nilai Model : ", m)
	} else {
		end := c.start + n
		if end > len(tableData) {
			e = errors.New("index out of range")
		} else {
			e = toolkit.Serde(tableData[0:n], m, "json")
		}
	}
	return e
}
