package rdbms

import (
	"database/sql"
	"errors"
	"github.com/eaciit/dbox"
	//"github.com/eaciit/errorlib"
	"github.com/eaciit/cast"
	"github.com/eaciit/hdc/hive"
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
	ResultType  string
	count       int
	start       int
	sessionHive *hive.Hive
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
	tableData := []toolkit.M{}
	var e error
	h := c.sessionHive
	if h != nil {
		// toolkit.Printf("n:%#v\n", c.sessionHive.Conn.Stdin)
		///// use exec only
		/*h.Conn.Open()
		result, e := h.Exec(c.QueryString)
		if e != nil {
			return e

		}

		for _, res := range result {
			var tmp toolkit.M
			h.ParseOutput(res, &tmp)
			tableData = append(tableData, tmp)
			// toolkit.Printf("n:%#v\n", tmp)
		}

		h.Conn.Close()*/

		/////// use populate
		//if c.QueryString == "SELECT code, description, total_emp, salary FROM sample_07" {
		c.sessionHive.Conn.Open()
		e := h.Populate(c.QueryString, &tableData)
		if e != nil {
			return e
		}
		c.sessionHive.Conn.Close()
		/*} else {
			c.sessionHive.Conn.Open()
			e := h.Populate("SELECT nama, sum(amount) as TotalAmount, avg(amount) as AverageAmount, max(amount) as MaxAmount FROM orders GROUP BY nama", &tableData)
			if e != nil {
		 		return e
			}
			c.sessionHive.Conn.Close()
		}*/

		/////// use exec line
		// var DoSomething = func(res string) {
		// 	fields := toolkit.M{}
		// 	h.ParseOutput(res, &fields)
		// 	tableData = append(tableData, fields)
		// }

		// e = h.ExecLine(c.QueryString, DoSomething)
		// if e != nil {
		// 	return e
		// }
		// toolkit.Printf("tabledata:%v\n", tableData)
	} else {

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
	}

	if e != nil {
		return e
	}
	if n == 0 {
		e = toolkit.Serde(tableData, m, "json")
	} else {
		end := c.start + n
		if end > len(tableData) {
			e = errors.New("index out of range")
		} else {
			e = toolkit.Serde(tableData[c.start:n], m, "json")
		}
	}

	return e
}
