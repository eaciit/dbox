package rdbms

import (
	"database/sql"
	"errors"
	"github.com/eaciit/dbox"
	//"github.com/eaciit/errorlib"
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
	DateFormat          string
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

func (c *Cursor) structValue(dataTypeList toolkit.M, col string, v interface{}) interface{} {
	for fieldname, datatype := range dataTypeList {
		if strings.ToLower(col) == fieldname {
			switch datatype.(string) {
			case "time.Time":
				val, e := time.Parse(c.DateFormat, toolkit.ToString(v))
				if e != nil {
					v = toolkit.ToString(v)
				} else {
					v = val
				}
			case "int", "int32", "int64":
				val, e := strconv.Atoi(toolkit.ToString(v))
				if e != nil {
					v = toolkit.ToString(v)
				} else {
					v = val
				}
			case "float", "float32", "float64":
				val, e := strconv.ParseFloat(toolkit.ToString(v), 64)
				if e != nil {
					v = toolkit.ToString(v)
				} else {
					v = val
				}
			case "bool":
				if c.driver == "mysql" {
					if toolkit.ToString(v) == "0" {
						v = false
					} else {
						v = true
					}
				} else {
					val, e := strconv.ParseBool(toolkit.ToString(v))
					if e != nil {
						v = toolkit.ToString(v)
					} else {
						v = val
					}
				}
			default:
				v = toolkit.ToString(v)
			}

		}
	}
	return v
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

		dataTypeList := toolkit.M{}
		var isStruct bool
		if valueType.Kind() == reflect.Struct {
			for i := 0; i < valueType.NumField(); i++ {
				namaField := strings.ToLower(valueType.Field(i).Name)
				dataType := valueType.Field(i).Type.String()
				dataTypeList.Set(namaField, dataType)
			}
			isStruct = true
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
				var ok bool
				var b []byte
				if val == nil {
					v = nil
				} else {
					b, ok = val.([]byte)
					if ok {
						v = string(b)
					} else {
						v = val
					}
				}
				/*mysql always byte, postgres only string that byte, oracle always string, mssql agree with field datatype*/
				if (ok && (c.driver == "mysql" || c.driver == "postgres")) || c.driver == "oci8" {
					if isStruct {
						v = c.structValue(dataTypeList, col, v)
					} else {
						intVal, e := strconv.Atoi(toolkit.ToString(v))
						if e != nil {
							e = nil
							floatVal, e := strconv.ParseFloat(toolkit.ToString(v), 64)
							if e != nil {
								e = nil
								boolVal, e := strconv.ParseBool(toolkit.ToString(v))
								if e != nil {
									e = nil
									dateVal, e := time.Parse(c.DateFormat, toolkit.ToString(v))
									if e != nil {
										v = v
									} else { /*if string is date*/
										v = dateVal
									}
								} else { /*if string is bool*/
									v = boolVal
								}
							} else { /*if string is float*/
								v = floatVal
							}
						} else { /*if string is int*/
							v = intVal
						}
					}
				}
				toolkit.Println(col, toolkit.TypeName(v), v)
				entry.Set(strings.ToLower(col), v)
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
