package odbc

import (
	// "database/sql"
	"github.com/eaciit/crowd"
	"github.com/eaciit/dbox"
	"github.com/eaciit/dbox/dbc/rdbms"
	// "github.com/eaciit/dbox.dev/dbc/odbc"
	err "github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"odbc"
	"strings"
)

const (
	modQuery = "Query"
)

type Query struct {
	dbox.Query
	// Sql        *sql.DB
	Sess       *odbc.Connection
	usePooling bool
	DriverDB   string
	count      int
}

func (q *Query) Session() *odbc.Connection {
	return q.Connection().(*Connection).Sess
}

func (q *Query) Prepare() error {
	return nil
}

func (q *Query) Cursor(in toolkit.M) (dbox.ICursor, error) {

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
				queryString = "SELECT " + setting.GetString("selectField") + " FROM " + setting.GetString("tableName")
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
		}
	}

	out, e := q.statement(queryString)
	if e != nil {
		return nil, err.Error(packageName, modQuery, "Cursor", e.Error())
	}
	// toolkit.Println(out)
	cursor.data = out.Get("data", "").(toolkit.Ms)
	cursor.count = out.Get("count", "").(int)
	cursor.Sess = q.Connection().(*Connection).Sess
	return cursor, nil
}

func (q *Query) Exec(param toolkit.M) error {
	_, e := q.prepare(param)
	if e != nil {
		return err.Error(packageName, modQuery, "Exec", e.Error())
	}

	return nil
}

func (q *Query) statement(query string) (toolkit.M, error) {
	toolkit.Println(query)
	out := toolkit.M{}
	tableData := toolkit.Ms{}
	// fieldName := []string{}

	// stmt, e := q.Connection().(*Connection).OdbcCon.Prepare(query)
	stmt, e := q.Connection().(*Connection).Sess.Prepare(query)
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

	/*nfields, e := stmt.NumFields()
	if e != nil {
		return nil, err.Error(packageName, modQuery, "statement", e.Error())
	}

	for i := 0; i < nfields; i++ {
		field, e := stmt.FieldMetadata(i + 1)
		if e != nil {
			return nil, err.Error(packageName, modQuery, "statement", e.Error())
		}
		fieldName = append(fieldName, field.Name)
	}*/
	// toolkit.Printf("%v\n", rows[0])
	for _, row := range rows {
		/*for i := 0; i < len(row.Data); i++ {
			rf := toolkit.TypeName(row.Data[i])
			if rf == "[]uint8" {
				row.Data[i] = toolkit.ToFloat64(string(row.Data[i].([]byte)), 2, toolkit.RoundingAuto)
			}

			if row.Data[i] == "fal" {
				row.Data[i] = false
			} else if row.Data[i] == "tru" {
				row.Data[i] = true
			}
		}*/

		/*entry := toolkit.M{}
		for i, data := range row.Data {
			entry.Set(fieldName[i], data)
		}

		tableData = append(tableData, entry)*/
		toolkit.Printf("%v\n", row.Data)
	}

	// toolkit.Println(tableData)
	out.Set("count", len(rows))
	out.Set("data", tableData)
	// toolkit.Println(tableData, query)

	return out, nil
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

	if freeQueryParts, hasFreeQuery := parts["freequery"]; hasFreeQuery {
		var syntax string
		qsyntax := freeQueryParts.([]*dbox.QueryPart)[0].Value.(interface{})
		syntax = qsyntax.(toolkit.M)["syntax"].(string)
		out.Set("freequery", syntax)
		out.Set("cmdType", dbox.QueryPartSelect)
	} else {
		var tableName string
		if fromParts, hasFrom := parts[dbox.QueryPartFrom]; hasFrom {
			tableName = fromParts.([]*dbox.QueryPart)[0].Value.(string)
		} else {

			return nil, err.Error(packageName, "Query", "prepare", "Invalid table name")
		}
		out.Set("tableName", tableName)

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
			out.Set("cmdType", dbox.QueryPartSelect)
		} else {
			if _, hasUpdate := parts[dbox.QueryPartUpdate]; hasUpdate {
				out.Set("cmdType", dbox.QueryPartUpdate)
			} else if _, hasInsert := parts[dbox.QueryPartInsert]; hasInsert {
				out.Set("cmdType", dbox.QueryPartInsert)
			} else if _, hasDelete := parts[dbox.QueryPartDelete]; hasDelete {
				out.Set("cmdType", dbox.QueryPartDelete)
			} else if _, hasSave := parts[dbox.QueryPartSave]; hasSave {
				out.Set("cmdType", dbox.QueryPartSave)
			} else {
				out.Set("cmdType", dbox.QueryPartSelect)
			}
		}
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
