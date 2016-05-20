package jdbc

import (
	"database/sql"
	"errors"
	"github.com/eaciit/cast"
	"github.com/eaciit/dbox"
	// "github.com/eaciit/hdc/hive"
	"github.com/eaciit/toolkit"
	"reflect"
	"strings"
)

const (
	modCursor = "Cursor"

	QueryResultCursor = "SQLCursor"
	QueryResultPipe   = "SQLPipe"
)

type Cursor struct {
	dbox.Cursor
	ResultType   string
	count, start int
	session      sql.DB
	QueryString  string
}

func (c *Cursor) Close() {
	c.session.Close()
}

func (c *Cursor) Count() int {
	return c.count
}

func (c *Cursor) ResetFetch() error {
	c.start = 0
	return nil
}

func (c *Cursor) Fetch(m interface{}, n int, closeWhenDone bool) error {
	tableData := []toolkit.M{}
	// var e error

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
			// b, ok := val.([]byte)

			// // toolkit.Println("i : ", i, " :col: ", col, " :val: ", val, " :b : ", b, " :type data : ", toolkit.Value(val))

			// var out interface{}
			// e = toolkit.Unjson(b, &out)
			// // toolkit.Println("i : ", i, "b : ", b, " :out: ", v, " :: error : ", e)

			// if e != nil {
			// 	ok = false
			// }
			// if ok {
			// 	v = out
			// 	toolkit.Println("error OK :: ", ok, " :v :", v)
			// } else {
			// 	toolkit.Println("error OK :: ", ok, " :b :", b)
			// 	v = string(b)
			// }
			v = val
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
	// toolkit.Println("... ::: ", tableData)

	maxIndex := toolkit.SliceLen(tableData)

	var e2 error
	if e2 != nil {
		return e2
	}
	end := c.start + n

	if end > maxIndex || n == 0 {
		end = maxIndex
	}

	if c.start >= maxIndex {
		e2 = errors.New("No more data to fetched!")
	} else {
		e2 = toolkit.Serde(tableData[c.start:end], m, "json")
	}
	c.start = end

	return e2
}
