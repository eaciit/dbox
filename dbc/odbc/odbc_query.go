package odbc

import (
	// "database/sql"
	// "encoding/binary"
	"github.com/eaciit/crowd"
	"github.com/eaciit/dbox"
	"github.com/eaciit/dbox/dbc/rdbms"
	err "github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	// "math"
	"odbc"
	"strconv"
	"strings"
	"time"
)

const (
	modQuery = "Query"
)

type Query struct {
	dbox.Query
	// Sql        *sql.DB
	Sess                             *odbc.Connection
	usePooling                       bool
	DriverDB, DateFormat, QStatement string
	count                            int
}

func (q *Query) Session() *odbc.Connection {
	return q.Connection().(*Connection).Sess
}

func (q *Query) Prepare() error {
	return nil
}

func (q *Query) Cursor(in toolkit.M) (dbox.ICursor, error) {
	q.Sess = q.Session()

	cursor := new(Cursor)
	var queryString string

	setting, e := q.prepare(in)
	if e != nil {
		return nil, err.Error(packageName, modQuery, "Cursor", e.Error())
	}

	if setting.GetString("cmdType") != dbox.QueryPartSelect {
		return nil, err.Error(packageName, modQuery, "Cursor", "Cursor is only working with select command, for "+setting.GetString("cmdType")+" please use .Exec instead")
	}

	if setting.Get("freequery", "").(string) != "" {
		queryString = setting.Get("freequery", "").(string)
	} else {
		if setting.Get("hasAggr", "").(bool) == true {
			if setting.GetString("selectField") != "" {
				queryString = "SELECT " + setting.GetString("selectField") + ", " + setting.GetString("aggr") + " FROM " + setting.GetString("tableName")
			} else {
				queryString = "SELECT " + setting.GetString("aggr") + " FROM " + setting.GetString("tableName")
			}
		} else {
			if setting.GetString("selectField") != "" {
				if (strings.ToLower(q.Connection().(*Connection).GetDriver()) == "oci8" &&
					setting.Get("isSkip").(bool)) || (strings.ToLower(q.Connection().(*Connection).GetDriver()) == "oci8" && setting.Get("isSkip").(bool) && setting.Get("isTake").(bool)) {
					splitField := strings.Split(setting.GetString("selectField"), ", ")
					field := toolkit.M{}
					for _, f := range splitField {
						field.Set(f, f)
					}
					_, idVal := toolkit.IdInfo(field)
					toolkit.Println(splitField, idVal)
					queryString = "SELECT " + setting.GetString("selectField") + ", rank() over(order by " + toolkit.ToString(idVal) + " asc) rn FROM " + setting.GetString("tableName")
				} else {
					queryString = "SELECT " + setting.GetString("selectField") + " FROM " + setting.GetString("tableName")
				}
			} else {
				queryString = "SELECT * FROM " + setting.GetString("tableName")
			}
		}

		if setting.Get("where", "") != nil {
			queryString += " WHERE " + setting.Get("where", "").(string)
		}

		if setting.Get("group", "") != "" {
			queryString += " GROUP BY " + setting.GetString("group")
		}

		if setting.Get("order", "") != "" {
			queryString += " ORDER BY " + setting.Get("order", "").(string)
		}

		if strings.ToLower(q.Connection().(*Connection).GetDriver()) == "mysql" {
			if setting.Get("isSkip").(bool) && setting.Get("isTake").(bool) {
				queryString += " LIMIT " + toolkit.ToString(setting.GetInt("take")) + " OFFSET " + toolkit.ToString(setting.GetInt("skip"))
			} else if setting.Get("isSkip").(bool) && !setting.Get("isTake").(bool) {
				queryString += " LIMIT " + toolkit.ToString(9999999) + " OFFSET " + toolkit.ToString(setting.GetInt("skip"))
			} else if setting.Get("isTake").(bool) && !setting.Get("isSkip").(bool) {
				queryString += " LIMIT " + toolkit.ToString(setting.GetInt("take"))
			}
		} else if strings.ToLower(q.Connection().(*Connection).GetDriver()) == "mssql" {
			if setting.Get("isSkip").(bool) && setting.Get("isTake").(bool) {
				queryString += " OFFSET " + toolkit.ToString(setting.GetInt("skip")) + " ROWS FETCH NEXT " + toolkit.ToString(setting.GetInt("take")) + " ROWS ONLY "
			} else if setting.Get("isSkip").(bool) && !setting.Get("isTake").(bool) {
				queryString += " OFFSET " + toolkit.ToString(setting.GetInt("skip")) + " ROWS"
			} else if setting.Get("isTake").(bool) && !setting.Get("isSkip").(bool) {
				top := "SELECT TOP " + toolkit.ToString(setting.GetInt("take")) + " "
				queryString = strings.Replace(queryString, "SELECT", top, 1)
			}
		} else if strings.ToLower(q.Connection().(*Connection).GetDriver()) == "postgres" {
			if setting.Get("isSkip").(bool) && setting.Get("isTake").(bool) {
				queryString += " LIMIT " + toolkit.ToString(setting.GetInt("take")) + " OFFSET " + toolkit.ToString(setting.GetInt("skip"))
			} else if setting.Get("isSkip").(bool) && !setting.Get("isTake").(bool) {
				queryString += " LIMIT ALL" + " OFFSET " + toolkit.ToString(setting.GetInt("skip"))
			} else if setting.Get("isTake").(bool) && !setting.Get("isSkip").(bool) {
				queryString += " LIMIT " + toolkit.ToString(setting.GetInt("take"))
			}
		} else if strings.ToLower(q.Connection().(*Connection).GetDriver()) == "oci8" {
			if setting.Get("isSkip").(bool) && setting.Get("isTake").(bool) {
				var lower, upper int
				upper = setting.GetInt("skip") + setting.GetInt("take")
				lower = upper - setting.GetInt("take") + 1

				queryString = "select * from (" + queryString + ") t1 WHERE t1.rn BETWEEN " + toolkit.ToString(lower) + " AND " + toolkit.ToString(upper)
			} else if setting.Get("isSkip").(bool) && !setting.Get("isTake").(bool) {
				queryString = "select * from (" + queryString +
					") t1 WHERE t1.rn > " + toolkit.ToString(setting.GetInt("skip"))
			} else if setting.Get("isTake").(bool) && !setting.Get("isSkip").(bool) {
				queryString = "select * from (" + queryString +
					") WHERE ROWNUM <= " + toolkit.ToString(setting.GetInt("take"))
			}
		}
	}

	q.QStatement = queryString
	out, e := q.Statement()
	if e != nil {
		return nil, err.Error(packageName, modQuery, "Cursor", e.Error())
	}
	// toolkit.Println(out)
	cursor.data = out.Get("data", "").(toolkit.Ms)
	cursor.count = out.Get("count", "").(int)
	cursor.Sess = q.Sess
	return cursor, nil
}

func (q *Query) Exec(param toolkit.M) error {
	setting, e := q.prepare(param)
	q.Sess = q.Session()
	commandType := toolkit.ToString(setting.Get("cmdType", ""))
	if e != nil {
		return err.Error(packageName, modQuery, "Exec", e.Error())
	}
	var tablename string
	if setting.Has("tableName") {
		tablename = toolkit.ToString(setting.Get("tableName", ""))
	}

	if toolkit.ToString(setting.Get("cmdType", "")) == dbox.QueryPartInsert {
		if setting.Has("fields") && setting.Has("values") {
			attributes := toolkit.ToString(setting.Get("fields", ""))
			values := toolkit.ToString(setting.Get("values", ""))
			if attributes != "" && values != "" {
				statement := "INSERT INTO " + tablename + " " + attributes + " VALUES " + values
				_, e = q.Sess.ExecDirect(statement)
				if e != nil && e.Error() != "" {
					return err.Error(packageName, modQuery+".Exec", commandType, e.Error())
				}

				if e = q.Sess.Commit(); e != nil && e.Error() != "" {
					return err.Error(packageName, modQuery+".Exec", commandType, e.Error())
				}
			}
		} else {
			return err.Error(packageName, modQuery+".Exec", commandType,
				"please provide the data")
		}
	} else if toolkit.ToString(setting.Get("cmdType", "")) == dbox.QueryPartUpdate {
		if setting.Has("setUpdate") {
			setUpdate := toolkit.ToString(setting.Get("setUpdate", ""))
			if setUpdate != "" {
				var statement string
				if setting.Has("where") {
					where := toolkit.ToString(setting.Get("where"))
					if where != "" {
						statement = "UPDATE " + tablename + " SET " + setUpdate +
							" WHERE " + where
					}
				} else {
					statement = "UPDATE " + tablename + " SET " + setUpdate
				}

				_, e = q.Sess.ExecDirect(statement)
				if e != nil && e.Error() != "" {
					return err.Error(packageName, modQuery+".Exec", commandType, e.Error())
				}
				if e = q.Sess.Commit(); e != nil && e.Error() != "" {
					return err.Error(packageName, modQuery+".Exec", commandType, e.Error())
				}
			}
		} else {
			return err.Error(packageName, modQuery+".Exec", commandType, "please provide the data")
		}
	} else if toolkit.ToString(setting.Get("cmdType", "")) == dbox.QueryPartSave {
		if setting.Has("fields") && setting.Has("values") {
			attributes := toolkit.ToString(setting.Get("fields", ""))
			values := toolkit.ToString(setting.Get("values", ""))
			if attributes != "" && values != "" {
				var querystmt string
				var where string
				if setting.Has("where") {
					where = toolkit.ToString(setting.Get("where"))
				}

				if where != "" {
					querystmt = "select 1 as data from " + tablename + " where " + where
				}

				var rowCount int
				if querystmt != "" {
					q.QStatement = querystmt
					out, e := q.Statement()
					if e != nil {
						return err.Error(packageName, modQuery, "Exec", e.Error())
					}
					rowCount = out.Get("count").(int)
				}

				var statement string
				if rowCount == 0 || where == "" {
					statement = "INSERT INTO " + tablename + " " + attributes + " VALUES " + values

				} else {
					if setting.Has("setUpdate") {
						setUpdate := toolkit.ToString(setting.Get("setUpdate", ""))
						if setUpdate != "" {
							statement = "UPDATE " + tablename + " SET " + setUpdate + " WHERE " + where
						}
					}
				}

				_, e = q.Sess.ExecDirect(statement)
				if e != nil && e.Error() != "" {
					return err.Error(packageName, modQuery+".Exec", commandType, e.Error())
				}
				if e = q.Sess.Commit(); e != nil && e.Error() != "" {
					return err.Error(packageName, modQuery+".Exec", commandType, e.Error())
				}
			} else if values == "" {
				return err.Error(packageName, modQuery+".Exec", commandType, "please provide the data")
			}
		}
	} else if commandType == dbox.QueryPartDelete {
		var statement string
		if setting.Has("where") {
			where := toolkit.ToString(setting.Get("where"))
			if where != "" {
				statement = "DELETE FROM " + tablename + " where " + where
			}
		} else {
			statement = "DELETE FROM " + tablename
		}

		_, e = q.Sess.ExecDirect(statement)
		if e != nil && e.Error() != "" {
			return err.Error(packageName, modQuery+".Exec", commandType, e.Error())
		}
		if e = q.Sess.Commit(); e != nil && e.Error() != "" {
			return err.Error(packageName, modQuery+".Exec", commandType, e.Error())
		}
	}

	return nil
}

func (q *Query) Statement() (toolkit.M, error) {
	toolkit.Println(q.QStatement)
	out := toolkit.M{}
	tableData := toolkit.Ms{}
	fieldName := []string{}
	q.DateFormat = q.Connection().(*Connection).dateFormat

	// stmt, e := q.Connection().(*Connection).OdbcCon.Prepare(query)
	stmt, e := q.Connection().(*Connection).Sess.Prepare(q.QStatement)
	if e != nil {
		return nil, err.Error(packageName, modQuery, "statement", e.Error())
	}
	defer stmt.Close()

	e = stmt.Execute()
	if e != nil {
		return nil, err.Error(packageName, modQuery, "statement", e.Error())
	}

	rows, e := stmt.FetchAll()
	if e != nil {
		return nil, err.Error(packageName, modQuery, "statement", e.Error())
	}

	nfields, e := stmt.NumFields()
	if e != nil {
		return nil, err.Error(packageName, modQuery, "statement", e.Error())
	}

	for i := 0; i < nfields; i++ {
		field, e := stmt.FieldMetadata(i + 1)
		if e != nil {
			return nil, err.Error(packageName, modQuery, "statement", e.Error())
		}
		fieldName = append(fieldName, field.Name)
	}

	for _, row := range rows {
		entry := toolkit.M{}
		for i := 0; i < len(row.Data); i++ {

			data := q.DataType(row.Data[i])
			entry.Set(fieldName[i], data)
		}

		tableData = append(tableData, entry)
	}

	out.Set("count", len(rows))
	out.Set("data", tableData)

	return out, nil
}

func (q *Query) DataType(data interface{}) interface{} {
	if data != nil {
		rf := toolkit.TypeName(data)
		// toolkit.Println("data>", rf)
		if rf == "[]uint8" {
			uintToString := string(data.([]uint8))
			spChar := strings.Contains(uintToString, "\x00")
			if spChar {
				uintToString = strings.Replace(uintToString, "\x00", "", 1)
			}

			floatVal, e := strconv.ParseFloat(uintToString, 64)
			if e != nil {

			} else {
				data = floatVal
			}
		} else {
			intVal, e := strconv.Atoi(toolkit.ToString(data))
			if e != nil {
				e = nil
				floatVal, e := strconv.ParseFloat(toolkit.ToString(data), 64)
				if e != nil {
					e = nil
					boolVal, e := strconv.ParseBool(toolkit.ToString(data))
					if e != nil {
						e = nil
						dateVal, e := time.Parse(q.DateFormat, toolkit.ToString(data))
						if e != nil {
							data = data
						} else { /*if string is date*/
							data = dateVal
						}
					} else { /*if string is bool*/
						data = boolVal
					}
				} else { /*if string is float*/
					data = floatVal
				}
			} else { /*if string is int*/
				data = intVal
			}
		}
	}

	return data
}

func StringValue(v interface{}, db string) string {
	var ret string
	switch v.(type) {
	case string:
		t, e := time.Parse(time.RFC3339, toolkit.ToString(v))
		if e != nil {
			ret = toolkit.Sprintf("%s", "'"+v.(string)+"'")
		} else {
			if strings.Contains(db, "oci8") {
				// toolkit.Println(t.Format("2006-01-02 15:04:05"))
				ret = "to_date('" + t.Format("02-01-2006 15:04:05") + "','DD-MM-YYYY hh24:mi:ss')"
			} else {
				ret = "'" + t.Format("2006-01-02 15:04:05") + "'"
			}
		}
	case time.Time:
		t := v.(time.Time).UTC()
		if strings.Contains(db, "oci8") {
			ret = "to_date('" + t.Format("2006-01-02 15:04:05") + "','yyyy-mm-dd hh24:mi:ss')"
		} else {
			ret = "'" + t.Format("2006-01-02 15:04:05") + "'"
		}
	case int, int32, int64, uint, uint32, uint64:
		ret = toolkit.Sprintf("%d", v.(int))
	case nil:
		ret = ""
	default:
		ret = toolkit.Sprintf("%v", v)
	}
	return ret
}

func (q *Query) prepare(in toolkit.M) (out toolkit.M, e error) {
	out = toolkit.M{}

	quyerParts := q.Parts()
	c := crowd.From(&quyerParts)
	groupParts := c.Group(func(x interface{}) interface{} {
		return x.(*dbox.QueryPart).PartType
	}, nil).Exec()

	parts := map[interface{}]interface{}{}
	if len(groupParts.Result.Data().([]crowd.KV)) > 0 {
		for _, kv := range groupParts.Result.Data().([]crowd.KV) {
			parts[kv.Key] = kv.Value
		}
	}

	_, hasUpdate := parts[dbox.QueryPartUpdate]
	_, hasInsert := parts[dbox.QueryPartInsert]
	_, hasDelete := parts[dbox.QueryPartDelete]
	_, hasSave := parts[dbox.QueryPartSave]
	_, hasFrom := parts[dbox.QueryPartFrom]
	procedureParts, hasProcedure := parts["procedure"]

	var tableName string
	if hasFrom {
		fromParts, _ := parts[dbox.QueryPartFrom]
		tableName = fromParts.([]*dbox.QueryPart)[0].Value.(string)
	} else {

		return nil, err.Error(packageName, "Query", "prepare", "Invalid table name")
	}
	out.Set("tableName", tableName)

	if freeQueryParts, hasFreeQuery := parts["freequery"]; hasFreeQuery {
		var syntax string
		qsyntax := freeQueryParts.([]*dbox.QueryPart)[0].Value.(interface{})
		syntax = qsyntax.(toolkit.M)["syntax"].(string)
		out.Set("freequery", syntax)
		out.Set("cmdType", dbox.QueryPartSelect)
	} else if hasInsert || hasUpdate || hasDelete || hasSave {
		if hasUpdate {
			out.Set("cmdType", dbox.QueryPartUpdate)
		} else if hasInsert {
			out.Set("cmdType", dbox.QueryPartInsert)
		} else if hasDelete {
			out.Set("cmdType", dbox.QueryPartDelete)
		} else if hasSave {
			out.Set("cmdType", dbox.QueryPartSave)
		}

		var where interface{}
		whereParts, hasWhere := parts[dbox.QueryPartWhere]
		if hasWhere {
			fb := q.Connection().Fb()
			for _, p := range whereParts.([]*dbox.QueryPart) {
				fs := p.Value.([]*dbox.Filter)
				for _, f := range fs {
					fb.AddFilter(f)
				}
			}

			where, e = fb.Build()
			if e != nil {

			}
			out.Set("where", where)
		}

		var dataM toolkit.M
		var dataMs []toolkit.M

		hasData := in.Has("data")
		var dataIsSlice bool
		if hasData {
			data := in.Get("data")
			if toolkit.IsSlice(data) {
				dataIsSlice = true
				e = toolkit.Unjson(toolkit.Jsonify(data), dataMs)
				if e != nil {
					return nil, err.Error(packageName, modQuery, "Exec: ", "Data encoding error: "+e.Error())
				}
			} else {
				dataM, e = toolkit.ToM(data)
				dataMs = append(dataMs, dataM)
				if e != nil {
					return nil, err.Error(packageName, modQuery, "Exec: ", "Data encoding error: "+e.Error())
				}
			}

			var id string
			var idVal interface{}
			if where == nil {
				id, idVal = toolkit.IdInfo(data)
				if id != "" {
					where = id + " = " + StringValue(idVal, "non")
				}
				out.Set("where", where)
			}

			if !dataIsSlice {
				var fields string
				var values string
				var setUpdate string
				var inc int
				for field, val := range dataM {
					stringval := StringValue(val, "non")
					if inc == 0 {
						fields = "(" + field
						values = "(" + stringval
						setUpdate = field + " = " + stringval
					} else {
						fields += ", " + field
						values += ", " + stringval
						setUpdate += ", " + field + " = " + stringval
					}
					inc++
				}
				fields += ")"
				values += ")"
				if hasInsert || hasSave {
					out.Set("fields", fields)
					out.Set("values", values)
				}
				if hasUpdate || hasSave {
					out.Set("setUpdate", setUpdate)
				}
			}
		}
	} else if hasProcedure {
		cmd := procedureParts.([]*dbox.QueryPart)[0].Value.(interface{})

		spName := cmd.(toolkit.M)["name"].(string) + " "
		params, hasParams := cmd.(toolkit.M)["params"]
		orderparam, hasOrder := cmd.(toolkit.M)["orderparam"]
		ProcStatement := ""
		toolkit.Println(spName, params, hasParams, orderparam, hasOrder, ProcStatement)

	} else {
		var selectField string
		incAtt := 0
		if selectParts, hasSelect := parts[dbox.QueryPartSelect]; hasSelect {
			for _, sl := range selectParts.([]*dbox.QueryPart) {
				for _, fid := range sl.Value.([]string) {
					if incAtt == 0 {
						selectField = fid
					} else {
						selectField = selectField + ", " + fid
					}
					incAtt++
				}
			}
		}
		out.Set("cmdType", dbox.QueryPartSelect)
		out.Set("selectField", selectField)

		///
		/// not yet iimplement
		var aggrExp string
		if aggrParts, hasAggr := parts[dbox.QueryPartAggr]; hasAggr {
			incAtt := 0
			for _, aggr := range aggrParts.([]*dbox.QueryPart) {
				/* isi qp :  &{AGGR {$sum 1 Total Item}}*/
				aggrInfo := aggr.Value.(dbox.AggrInfo)
				/* isi Aggr Info :  {$sum 1 Total Item}*/

				if incAtt == 0 {
					aggrExp = strings.Replace(aggrInfo.Op, "$", "", 1) + "(" +
						toolkit.ToString(aggrInfo.Field) + ")" + " as \"" + aggrInfo.Alias + "\""
				} else {
					aggrExp += ", " + strings.Replace(aggrInfo.Op, "$", "", 1) + "(" +
						toolkit.ToString(aggrInfo.Field) + ")" + " as \"" + aggrInfo.Alias + "\""
				}
				incAtt++
			}
		}
		out.Set("aggr", aggrExp)

		///
		/// Where Condition
		var where interface{}
		if whereParts, hasWhere := parts[dbox.QueryPartWhere]; hasWhere {
			fb := q.Connection().Fb()
			for _, p := range whereParts.([]*dbox.QueryPart) {
				for _, f := range p.Value.([]*dbox.Filter) {
					if in != nil {
						f = rdbms.ReadVariable(f, in)
					}
					fb.AddFilter(f)
				}
			}
			where, e = fb.Build()
			if e != nil {
				return nil, err.Error(packageName, modQuery, "prepare", e.Error())
			}
		}
		out.Set("where", where)

		///
		/// Sort Condition
		var sort []string
		if sortParts, hasSort := parts[dbox.QueryPartOrder]; hasSort {
			sort = []string{}
			for _, sr := range sortParts.([]*dbox.QueryPart) {
				for _, s := range sr.Value.([]string) {
					sort = append(sort, s)
				}
			}
		}
		out.Set("sort", sort)

		///
		/// Take Condition
		take := 0
		isTake := false
		if takeParts, hasTake := parts[dbox.QueryPartTake]; hasTake {
			isTake = true
			take = takeParts.([]*dbox.QueryPart)[0].Value.(int)
		}
		out.Set("isTake", isTake)
		out.Set("take", take)

		///
		/// Skip Condition
		skip := 0
		isSkip := false
		if skipParts, hasSkip := parts[dbox.QueryPartSkip]; hasSkip {
			isSkip = true
			skip = skipParts.([]*dbox.QueryPart)[0].Value.(int)
		}
		out.Set("isSkip", isSkip)
		out.Set("skip", skip)

		///
		/// Group By Condition
		var groupExp string
		hasAggr := false
		if groupParts, hasGroup := parts[dbox.QueryPartGroup]; hasGroup {
			hasAggr = true
			for _, pg := range groupParts.([]*dbox.QueryPart) {
				for i, grValue := range pg.Value.([]string) {
					if i == 0 {
						groupExp += grValue
					} else {
						groupExp += ", " + grValue
					}
				}
			}
		}
		out.Set("group", groupExp)
		out.Set("hasAggr", hasAggr)

		///
		/// Order By Condition
		var orderExp string
		if orderParts, hasOrder := parts[dbox.QueryPartOrder]; hasOrder {
			for _, ordrs := range orderParts.([]*dbox.QueryPart) {
				for i, oVal := range ordrs.Value.([]string) {
					if i == 0 {
						if string(oVal[0]) == "-" {
							orderExp = strings.Replace(oVal, "-", "", 1) + " DESC"
						} else {
							orderExp = oVal + " ASC"
						}
					} else {
						if string(oVal[0]) == "-" {
							orderExp += ", " + strings.Replace(oVal, "-", "", 1) + " DESC"
						} else {
							orderExp += ", " + oVal + " ASC"
						}
					}
				}
			}
		}
		out.Set("order", orderExp)
	}

	return
}

func (q *Query) Close() {
	// q.Sql.Close()
	q.Sess.Close()
}
