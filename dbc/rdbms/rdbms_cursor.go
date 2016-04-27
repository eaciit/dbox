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
	"strconv"
	"strings"
	"time"
)

const (
	modCursor = "Cursor"

	QueryResultCursor = "SQLCursor"
	QueryResultPipe   = "SQLPipe"
)

type Cursor struct {
	dbox.Cursor
	ResultType          string
	count, start        int
	sessionHive         *hive.Hive
	session             sql.DB
	QueryString, driver string
}

func (c *Cursor) Close() {
	h := c.sessionHive
	if h != nil {
		if h.Conn.Open() != nil {
			h.Conn.Close()
		}
	}
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
		e = h.Exec(c.QueryString, func(x hive.HiveResult) error {
			tableData = append(tableData, x.ResultObj.(map[string]interface{}))
			return nil
		})
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

				if ok { /*mysql always byte, postgres only string that byte, oracle always string, mssql agree with field datatype*/
					v = string(b)
					intVal, e := strconv.Atoi(toolkit.ToString(v))
					if e != nil {
						e = nil
						floatVal, e := strconv.ParseFloat(toolkit.ToString(v), 64)
						if e != nil {
							e = nil
							dateVal, e := time.Parse("2006-01-02 15:04:05", toolkit.ToString(v))
							if e != nil {
								v = string(b)
							} else { /*if string is float*/
								v = dateVal
							}
						} else { /*if string is float*/
							v = floatVal
						}
					} else { /*if string is int*/
						v = intVal
					}
				} else {
					if c.driver == "oci8" {
						if val == nil {
							v = nil
						} else if strings.Contains(toolkit.TypeName(val), "string") {
							intVal, e := strconv.Atoi(toolkit.ToString(val))
							if e != nil {
								e = nil
								floatVal, e := strconv.ParseFloat(toolkit.ToString(val), 64)
								if e != nil {
									v = val
								} else { /*if string is float*/
									v = floatVal
								}
							} else { /*if string is int*/
								v = intVal
							}
						} else { /*if not string*/
							v = val
						}
					} else { /*if not oracle*/
						v = val
					}
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
							// entry.Set(namaField, cast.String2Date(cast.ToString(entry[namaField]), "2006-01-02 15:04:05"))
							entry.Set(namaField, entry[namaField])
						}
					}
				}
			}

			tableData = append(tableData, entry)
		}
	}
	maxIndex := toolkit.SliceLen(tableData)

	if e != nil {
		return e
	}
	end := c.start + n

	if end > maxIndex || n == 0 {
		end = maxIndex
	}

	if c.start >= maxIndex {
		e = errors.New("No more data to fetched!")
	} else {
		e = toolkit.Serde(tableData[c.start:end], m, "json")
	}
	c.start = end

	return e
}
