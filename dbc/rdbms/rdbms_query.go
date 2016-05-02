package rdbms

import (
	"database/sql"
	//"encoding/json"
	"fmt"
	"github.com/eaciit/cast"
	// "github.com/eaciit/crowd.old"
	"github.com/eaciit/crowd"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/hdc/hive"
	"github.com/eaciit/toolkit"
	"reflect"
	"strings"
	"time"
)

const (
	modQuery = "Query"
)

type Query struct {
	dbox.Query
	Hive       *hive.Hive
	Sql        sql.DB
	usePooling bool
	DriverDB   string
	count      int
}

func (q *Query) Session() sql.DB {
	q.usePooling = q.Config("pooling", false).(bool)
	// if q.Sql == nil {
	if q.usePooling {
		q.Sql = q.Connection().(*Connection).Sql
	} else {
		q.Sql = q.Connection().(*Connection).Sql
	}
	// }
	return q.Sql
}

func (q *Query) SessionHive() *hive.Hive {
	q.Hive = q.Connection().(*Connection).Hive
	return q.Hive
}

func (q *Query) GetDriverDB() string {
	q.DriverDB = q.Connection().(*Connection).Drivername
	return q.DriverDB
}

func (q *Query) Close() {
	if q.GetDriverDB() != "hive" {
		q.Sql.Close()
	} else {
		// if q.SessionHive().Conn.Open() != nil {
		// 	q.SessionHive().Conn.Close()
		// }
	}
}

func (q *Query) Prepare() error {
	return nil
}

func StringValue(v interface{}, db string) string {
	var ret string
	switch v.(type) {
	case string:
		t, e := time.Parse(time.RFC3339, toolkit.ToString(v))
		if e != nil {
			ret = fmt.Sprintf("%s", "'"+v.(string)+"'")
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
		ret = fmt.Sprintf("%d", v.(int))
	case nil:
		ret = ""
	default:
		ret = fmt.Sprintf("%v", v)
	}
	return ret
}

func ReadVariable(f *dbox.Filter, in toolkit.M) *dbox.Filter {
	if (f.Op == "$and" || f.Op == "$or") &&
		strings.Contains(reflect.TypeOf(f.Value).String(), "dbox.Filter") {
		fs := f.Value.([]*dbox.Filter)
		/* nilai fs :  [0xc082059590 0xc0820595c0]*/
		for i, ff := range fs {
			/* nilai ff[0] : &{umur $gt @age} && ff[1] : &{name $eq @nama}*/
			bf := ReadVariable(ff, in)
			/* nilai bf[0] :  &{umur $gt 25} && bf[1] : &{name $eq Kane}*/
			fs[i] = bf
		}
		f.Value = fs
		return f
	} else {
		if reflect.TypeOf(f.Value).Kind() == reflect.Slice {
			if strings.Contains(reflect.TypeOf(f.Value).String(), "interface") {
				fSlice := f.Value.([]interface{})
				/*nilai fSlice : [@name1 @name2]*/
				for i := 0; i < len(fSlice); i++ {
					/* nilai fSlice [i] : @name1*/
					if string(cast.ToString(fSlice[i])[0]) == "@" {
						for key, val := range in {
							if cast.ToString(fSlice[i]) == key {
								fSlice[i] = val
							}
						}
					}
				}
				f.Value = fSlice
			} else if strings.Contains(reflect.TypeOf(f.Value).String(), "string") {
				fSlice := f.Value.([]string)
				for i := 0; i < len(fSlice); i++ {
					if string(fSlice[i][0]) == "@" {
						for key, val := range in {
							if fSlice[i] == key {
								fSlice[i] = val.(string)
							}
						}
					}
				}
				f.Value = fSlice
			}
			return f
		} else {
			if string(cast.ToString(f.Value)[0]) == "@" {
				for key, val := range in {
					if cast.ToString(f.Value) == key {
						f.Value = val
					}
				}
			}
			return f
		}
	}
	return f
}

func (q *Query) Cursor(in toolkit.M) (dbox.ICursor, error) {
	var e error
	/*
		if q.Parts == nil {
			return nil, errorlib.Error(packageName, modQuery,
				"Cursor", fmt.Sprintf("No Query Parts"))
		}
	*/

	dbname := q.Connection().Info().Database
	cursor := dbox.NewCursor(new(Cursor))
	if q.GetDriverDB() == "hive" {
		session := q.SessionHive()
		cursor.(*Cursor).sessionHive = session
	} else {
		session := q.Session()
		cursor.(*Cursor).session = session
	}
	cursor.(*Cursor).DateFormat = q.Connection().(*Connection).DateFormat
	driverName := q.GetDriverDB()
	// driverName = "oracle"
	var QueryString string

	/*
		parts will return E - map{interface{}}interface{}
		where each interface{} returned is slice of interfaces --> []interface{}
	*/
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

	fromParts, hasFrom := parts[dbox.QueryPartFrom]
	procedureParts, hasProcedure := parts["procedure"]
	freeQueryParts, hasFreeQuery := parts["freequery"]

	idMap := toolkit.M{}
	if hasFrom {
		tablename := ""
		tablename = fromParts.([]*dbox.QueryPart)[0].Value.(string)

		selectParts, hasSelect := parts[dbox.QueryPartSelect]
		var attribute string
		incAtt := 0
		if hasSelect {
			for _, sl := range selectParts.([]*dbox.QueryPart) {
				for _, fid := range sl.Value.([]string) {
					if incAtt == 0 {
						attribute = fid
					} else {
						attribute = attribute + ", " + fid
					}

					idMap.Set(fid, fid)
					incAtt++
				}
			}
		} else {
			_, hasUpdate := parts[dbox.QueryPartUpdate]
			_, hasInsert := parts[dbox.QueryPartInsert]
			_, hasDelete := parts[dbox.QueryPartDelete]
			_, hasSave := parts[dbox.QueryPartSave]

			if hasUpdate || hasInsert || hasDelete || hasSave {
				return nil, errorlib.Error(packageName, modQuery, "Cursor",
					"Valid operation for a cursor is select only")
			}
		}

		aggrParts, hasAggr := parts[dbox.QueryPartAggr]

		var aggrExpression string
		if hasAggr {
			incAtt := 0
			// for _, aggr := range aggrParts.([]interface{}) {
			for _, aggr := range aggrParts.([]*dbox.QueryPart) {
				// qp := aggr.(*dbox.QueryPart)
				/* isi qp :  &{AGGR {$sum 1 Total Item}}*/
				aggrInfo := aggr.Value.(dbox.AggrInfo)
				/* isi Aggr Info :  {$sum 1 Total Item}*/

				if incAtt == 0 {
					if driverName == "hive" {
						aggrExpression = strings.Replace(aggrInfo.Op, "$", "", 1) + "(" +
							cast.ToString(aggrInfo.Field) + ")" + " as " + aggrInfo.Alias
					} else {
						aggrExpression = strings.Replace(aggrInfo.Op, "$", "", 1) + "(" +
							cast.ToString(aggrInfo.Field) + ")" + " as \"" + aggrInfo.Alias + "\""
					}
				} else {
					if driverName == "hive" {
						aggrExpression += ", " + strings.Replace(aggrInfo.Op, "$", "", 1) + "(" +
							cast.ToString(aggrInfo.Field) + ")" + " as " + aggrInfo.Alias
					} else {
						aggrExpression += ", " + strings.Replace(aggrInfo.Op, "$", "", 1) + "(" +
							cast.ToString(aggrInfo.Field) + ")" + " as \"" + aggrInfo.Alias + "\""
					}
				}
				incAtt++
			}
			/* isi Aggr Expression :  sum(1) as 'Total Item', max(amount) as 'Max Amount', avg(amount) as 'Average Amount'*/
		}

		var where interface{}
		whereParts, hasWhere := parts[dbox.QueryPartWhere]
		if hasWhere {
			fb := q.Connection().Fb()
			for _, p := range whereParts.([]*dbox.QueryPart) {
				fs := p.Value.([]*dbox.Filter)
				for _, f := range fs {
					if in != nil {
						f = ReadVariable(f, in)
					}
					fb.AddFilter(f)
				}
			}
			where, e = fb.Build()
			if e != nil {
				return nil, errorlib.Error(packageName, modQuery, "Cursor",
					e.Error())
			}
		}

		var orderExpression string
		orderParts, hasOrder := parts[dbox.QueryPartOrder]
		if hasOrder {
			qp := orderParts.([]*dbox.QueryPart)[0]
			for i, fid := range qp.Value.([]string) {
				if i == 0 {
					if string(fid[0]) == "-" {
						orderExpression = strings.Replace(fid, "-", "", 1) + " DESC"
					} else {
						orderExpression = fid + " ASC"
					}
				} else {
					if string(fid[0]) == "-" {
						orderExpression += ", " + strings.Replace(fid, "-", "", 1) + " DESC"
					} else {
						orderExpression += ", " + fid + " ASC"
					}
				}
			}
			// for _, oval := range orderParts.([]interface{}) {
			// 	qp := oval.(*dbox.QueryPart)
			// 	for i, fid := range qp.Value.([]string) {
			// 		if i == 0 {
			// 			if string(fid[0]) == "-" {
			// 				orderExpression = strings.Replace(fid, "-", "", 1) + " DESC"
			// 			} else {
			// 				orderExpression = fid + " ASC"
			// 			}
			// 		} else {
			// 			if string(fid[0]) == "-" {
			// 				orderExpression += ", " + strings.Replace(fid, "-", "", 1) + " DESC"
			// 			} else {
			// 				orderExpression += ", " + fid + " ASC"
			// 			}
			// 		}
			// 	}
			// }
		}

		skip := 0
		skipParts, hasSkip := parts[dbox.QueryPartSkip]
		if hasSkip {
			skip = skipParts.([]*dbox.QueryPart)[0].Value.(int)
		}

		take := 0
		takeParts, hasTake := parts[dbox.QueryPartTake]
		if hasTake {
			take = takeParts.([]*dbox.QueryPart)[0].Value.(int)
		}

		partGroup, hasGroup := parts[dbox.QueryPartGroup]
		var groupExpression string
		if hasGroup {
			for _, aggr := range partGroup.([]*dbox.QueryPart) {
				// qp := aggr.(*dbox.QueryPart)
				groupValue := aggr.Value.([]string)
				for i, val := range groupValue {
					if i == 0 {
						groupExpression += val
					} else {
						groupExpression += ", " + val
					}
				}
			}
			/* isi group expression :  GROUP BY nama*/
		}

		if dbname != "" && tablename != "" && e != nil && skip == 0 && take == 0 && where == nil {

		}
		if hasAggr {
			if hasSelect && attribute != "" {
				QueryString = "SELECT " + attribute + ", " + aggrExpression + " FROM " + tablename
			} else {
				QueryString = "SELECT " + aggrExpression + " FROM " + tablename
			}

		} else {
			if attribute == "" {
				QueryString = "SELECT * FROM " + tablename
			} else {
				if driverName == "oci8" {
					_, idVal := toolkit.IdInfo(idMap)
					QueryString = "SELECT " + attribute + ", rank() over(order by " + idVal.(string) + " asc) rn FROM " + tablename
				} else {
					QueryString = "SELECT " + attribute + " FROM " + tablename
				}
			}
		}

		if hasWhere {
			QueryString += " WHERE " + cast.ToString(where)
		}
		if hasGroup {
			QueryString += " GROUP BY " + groupExpression
		}
		if hasOrder {
			QueryString += " ORDER BY " + orderExpression
		}

		if driverName == "mysql" {
			if hasSkip && hasTake {
				QueryString += " LIMIT " + cast.ToString(take) +
					" OFFSET " + cast.ToString(skip)
			} else if hasSkip && !hasTake {
				QueryString += " LIMIT " + cast.ToString(9999999) +
					" OFFSET " + cast.ToString(skip)
			} else if hasTake && !hasSkip {
				QueryString += " LIMIT " + cast.ToString(take)
			}
		} else if driverName == "mssql" {
			if hasSkip && hasTake {
				QueryString += " OFFSET " + cast.ToString(skip) + " ROWS FETCH NEXT " +
					cast.ToString(take) + " ROWS ONLY "
			} else if hasSkip && !hasTake {
				QueryString += " OFFSET " + cast.ToString(skip) + " ROWS"
			} else if hasTake && !hasSkip {
				top := "SELECT TOP " + cast.ToString(take) + " "
				QueryString = strings.Replace(QueryString, "SELECT", top, 1)
			}

		} else if driverName == "oci8" {
			if hasSkip && hasTake {
				var lower, upper int
				upper = skip + take
				lower = upper - take + 1

				QueryString = "select * from (" + QueryString + ") t1 WHERE t1.rn BETWEEN " + cast.ToString(lower) + " AND " + cast.ToString(upper)
			} else if hasSkip && !hasTake {
				QueryString = "select * from (" + QueryString +
					") t1 WHERE t1.rn > " + cast.ToString(skip)
			} else if hasTake && !hasSkip {
				QueryString = "select * from (" + QueryString +
					") WHERE ROWNUM <= " + cast.ToString(take)
			}

		} else if driverName == "postgres" {
			if hasSkip && hasTake {
				QueryString += " LIMIT " + cast.ToString(take) +
					" OFFSET " + cast.ToString(skip)
			} else if hasSkip && !hasTake {
				QueryString += " LIMIT ALL" +
					" OFFSET " + cast.ToString(skip)
			} else if hasTake && !hasSkip {
				QueryString += " LIMIT " + cast.ToString(take)
			}
		} else if driverName == "hive" {
			if hasSkip && hasTake {
				var lower, upper int
				upper = skip + take
				lower = upper - take

				//toolkit.Println("take: ", take, "skip: ", skip, "lower: ", lower, "upper: ", upper)
				QueryString = "SELECT " + attribute + " FROM (SELECT *,row_number() over () as rowid FROM " + tablename + ") x WHERE rowid > " +
					toolkit.ToString(lower) + " and rowid <= " + toolkit.ToString(upper)
			} else if hasSkip && !hasTake {

			} else if hasTake && !hasSkip {
				QueryString += " LIMIT " + toolkit.ToString(take)
			}
		}
		// toolkit.Println(QueryString)
		var querystmt string
		if where != nil {
			querystmt = "select count(*) from " + tablename +
				" where " + cast.ToString(where)
		} else {
			querystmt = "select count(*) from " + tablename
		}

		/*populate fetch.count*/
		var rowCount int
		if driverName == "hive" {
			// rowCount = 999999999
			// row := sessionHive.Exec(querystmt)
			// rowCount = toolkit.ToInt(row[0], "auto")
		} else {
			rows, _ := cursor.(*Cursor).session.Query(querystmt)
			for rows.Next() {
				rows.Scan(&rowCount)
			}
		}
		if rowCount <= skip {
			rowCount = 0
		} else {
			rowCount -= skip
		}
		if rowCount >= take && take > 0 {
			rowCount = take
		}
		cursor.(*Cursor).count = rowCount
		cursor.(*Cursor).driver = driverName
		/*assign cursor.QueryString*/
		cursor.(*Cursor).QueryString = QueryString
	} else if hasProcedure {
		procCommand := procedureParts.([]*dbox.QueryPart)[0].Value.(interface{})

		spName := procCommand.(toolkit.M)["name"].(string) + " "
		params, hasParam := procCommand.(toolkit.M)["parms"]
		orderparam, hasOrder := procCommand.(toolkit.M)["orderparam"]
		ProcStatement := ""

		if driverName == "mysql" {
			paramstring := ""
			if hasParam && hasOrder {
				paramToolkit := params.(toolkit.M)
				orderString := orderparam.([]string)
				for i := 0; i < len(paramToolkit); i++ {
					if i == 0 {
						if strings.Contains(orderString[i], "@@") {
							paramstring = "(" + strings.Replace(orderString[i], "@@", "@", 1)
						} else if StringValue(paramToolkit[orderString[i]], driverName) != "''" {
							paramstring = "(" + StringValue(paramToolkit[orderString[i]], driverName)

						} else {
							paramstring = "("
						}

					} else {
						if strings.Contains(orderString[i], "@@") {
							paramstring += ", " + strings.Replace(orderString[i], "@@", "@", 1)
						} else {
							paramstring += ", " + StringValue(paramToolkit[orderString[i]], driverName)

						}
					}
				}

			} else if hasParam && !hasOrder {
				return nil, errorlib.Error(packageName, modQuery, "procedure", "please provide order parameter")
			} else {
				paramstring = "("
			}
			paramstring += ");"

			ProcStatement = "CALL " + spName + paramstring
		} else if driverName == "mssql" {
			paramstring := ""
			incParam := 0
			if hasParam {
				for key, val := range params.(toolkit.M) {
					if key != "" {
						if incParam == 0 {
							paramstring = key + " = " + StringValue(val, driverName) + ""

						} else {
							paramstring += ", " + key + " = " + StringValue(val, driverName) + ""
						}
						incParam += 1
					}
				}
				paramstring += ";"
			}

			ProcStatement = "EXECUTE " + spName + paramstring
		} else if driverName == "oci8" {
			var paramstring string
			var variable string
			var isEmpty bool
			if hasParam && hasOrder {
				paramToolkit := params.(toolkit.M)
				orderString := orderparam.([]string)
				for i := 0; i < len(paramToolkit); i++ {
					if i == 0 {
						if strings.Contains(orderString[i], "@@") {
							variable = "var " + strings.Replace(orderString[i], "@@", "", 1) +
								" " + cast.ToString(paramToolkit[orderString[i]]) + ";"
							paramstring = "(" + strings.Replace(orderString[i], "@@", ":", 1)
							isEmpty = false
						} else if StringValue(paramToolkit[orderString[i]], driverName) != "''" {
							paramstring = "(" + StringValue(paramToolkit[orderString[i]], driverName)
							isEmpty = false
						}

					} else {
						if strings.Contains(orderString[i], "@@") {
							variable += "var " + strings.Replace(orderString[i], "@@", "", 1) +
								" " + cast.ToString(paramToolkit[orderString[i]]) + ";"
							paramstring += ", " + strings.Replace(orderString[i], "@@", ":", 1)
						} else {
							paramstring += ", " + StringValue(paramToolkit[orderString[i]], driverName)

						}
					}
				}
				if !isEmpty {
					paramstring += ");"
				}
			} else if hasParam && !hasOrder {
				return nil, errorlib.Error(packageName, modQuery, "procedure", "please provide order parameter")
			}

			ProcStatement = variable + "EXECUTE " + spName + paramstring
			// toolkit.Println("ProcStatement>", ProcStatement)
		} else if driverName == "postgres" {
			paramstring := ""
			if hasParam && hasOrder {
				paramToolkit := params.(toolkit.M)
				orderString := orderparam.([]string)
				for i := 0; i < len(paramToolkit); i++ {
					if i == 0 {
						if strings.Contains(orderString[i], "@@") {
							paramstring = "(" + strings.Replace(orderString[i], "@@", "@", 1)
						} else if StringValue(paramToolkit[orderString[i]], driverName) != "''" {
							paramstring = "(" + StringValue(paramToolkit[orderString[i]], driverName)

						} else {
							paramstring = "("
						}

					} else {
						if strings.Contains(orderString[i], "@@") {
							paramstring += ", " + strings.Replace(orderString[i], "@@", "@", 1)
						} else {
							paramstring += ", " + StringValue(paramToolkit[orderString[i]], driverName)

						}
					}

					// fmt.Println("Print value order", paramstring)
				}

			} else if hasParam && !hasOrder {

				return nil, errorlib.Error(packageName, modQuery, "procedure", "please provide order parameter")

			} else {
				paramstring = "("
			}
			paramstring += ")"

			ProcStatement = "SELECT " + spName + paramstring
		}

		cursor.(*Cursor).QueryString = ProcStatement

		// fmt.Println("Proc Statement : ", ProcStatement)
	} else if hasFreeQuery {
		querySyntax := freeQueryParts.([]*dbox.QueryPart)[0].Value.(interface{})
		syntax := querySyntax.(toolkit.M)["syntax"].(string)
		cursor.(*Cursor).QueryString = syntax
	}
	return cursor, nil
}

func (q *Query) Exec(parm toolkit.M) error {
	var e error
	if parm == nil {
		parm = toolkit.M{}
	}

	dbname := q.Connection().Info().Database
	driverName := q.GetDriverDB()
	// driverName = "oracle"
	tablename := ""
	data := parm.Get("data", nil)
	// fmt.Println("Hasil ekstraksi Param : ", data)

	/*========================EXTRACT FIELD, DATA AND FORMAT DATE=============================*/

	var attributes string
	var values string
	var setUpdate, statement string
	var i int

	dataM, e := toolkit.ToM(data)
	if e != nil {
		return errorlib.Error(packageName, modQuery, "Exec: data extraction", "Data encoding error: "+e.Error())
	}

	if data != nil {
		for field, val := range dataM {
			namaField := field
			dataValues := val
			stringValues := StringValue(dataValues, driverName)
			if i == 0 {
				attributes = "(" + namaField
				setUpdate = namaField + " = " + stringValues
				values = "(" + stringValues

			} else {
				attributes += ", " + namaField
				setUpdate += ", " + namaField + " = " + stringValues
				values += ", " + stringValues

			}
			i += 1
		}
		attributes += ")"
		values += ")"
	}

	/*=================================END OF EXTRACTION=======================================*/

	temp := ""
	quyerParts := q.Parts()
	c := crowd.From(&quyerParts)
	groupParts := c.Group(func(x interface{}) interface{} {
		qp := x.(*dbox.QueryPart)
		temp = toolkit.JsonString(qp)
		return qp.PartType
	}, nil).Exec()
	parts := map[interface{}]interface{}{}
	if len(groupParts.Result.Data().([]crowd.KV)) > 0 {
		for _, kv := range groupParts.Result.Data().([]crowd.KV) {
			parts[kv.Key] = kv.Value
		}
	}

	fromParts, hasFrom := parts[dbox.QueryPartFrom]
	if !hasFrom {

		return errorlib.Error(packageName, "Query", modQuery, "Invalid table name")
	}
	tablename = fromParts.([]*dbox.QueryPart)[0].Value.(string)

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

		} else {

		}

	}
	commandType := ""
	multi := false

	_, hasDelete := parts[dbox.QueryPartDelete]
	_, hasInsert := parts[dbox.QueryPartInsert]
	_, hasUpdate := parts[dbox.QueryPartUpdate]
	_, hasSave := parts[dbox.QueryPartSave]

	if hasDelete {
		commandType = dbox.QueryPartDelete
	} else if hasInsert {
		commandType = dbox.QueryPartInsert
	} else if hasUpdate {
		commandType = dbox.QueryPartUpdate
	} else if hasSave {
		commandType = dbox.QueryPartSave
	}

	var id string
	var idVal interface{}
	if data == nil {
		multi = true
	} else {
		if where == nil {
			id, idVal = toolkit.IdInfo(data)
			if id != "" {
				where = id + " = " + StringValue(idVal, "non")
			}
		} else {
			multi = true
		}
	}

	session := q.Session()
	sessionHive := q.SessionHive()

	if dbname != "" && tablename != "" && multi == true {

	}
	if commandType == dbox.QueryPartInsert {
		if attributes != "" && values != "" {
			if driverName == "hive" {
				statement = "INSERT INTO " + tablename + " VALUES " + values
				e = sessionHive.Exec(statement, nil)
			} else {
				statement = "INSERT INTO " + tablename + " " + attributes + " VALUES " + values
				_, e = session.Exec(statement)
			}

			if e != nil {
				fmt.Println(e.Error())
			}
		} else {
			return errorlib.Error(packageName, modQuery+".Exec", commandType,
				"please provide the data")
		}

	} else if commandType == dbox.QueryPartUpdate {
		if setUpdate != "" {
			var statement string
			if where != nil {
				statement = "UPDATE " + tablename + " SET " + setUpdate +
					" WHERE " + cast.ToString(where)
			} else {
				statement = "UPDATE " + tablename + " SET " + setUpdate
			}
			if driverName == "hive" {
				e = sessionHive.Exec(statement, nil)
			} else {
				_, e = session.Exec(statement)
			}
			if e != nil {
				return errorlib.Error(packageName, modQuery+".Exec", commandType,
					cast.ToString(e.Error()))
			}
		} else {
			return errorlib.Error(packageName, modQuery+".Exec", commandType,
				"please provide the data")
		}

	} else if commandType == dbox.QueryPartDelete {
		var statement string
		if where != nil {
			statement = "DELETE FROM " + tablename + " where " + cast.ToString(where)
		} else {
			statement = "DELETE FROM " + tablename
		}
		if driverName == "hive" {
			e = sessionHive.Exec(statement, nil)
		} else {
			_, e = session.Exec(statement)
		}
		if e != nil {
			return errorlib.Error(packageName, modQuery+".Exec", commandType,
				cast.ToString(e.Error()))
		}
	} else if commandType == dbox.QueryPartSave {
		if attributes != "" && values != "" {
			var querystmt string
			if where != nil {
				querystmt = "select 1 as data from " + tablename + " where " + cast.ToString(where)
			}
			var rowCount int
			if driverName == "hive" {
				rowCount = 0
				// row := sessionHive.Exec(querystmt, nil)
				// rowCount = toolkit.ToInt(row[0], "auto")
			} else {
				if querystmt != "" {
					rows, _ := session.Query(querystmt)
					for rows.Next() {
						rows.Scan(&rowCount)
					}
				}
			}

			var statement string
			if rowCount == 0 || where == nil {
				if driverName == "hive" {
					statement = "INSERT INTO " + tablename + " VALUES " + values
				} else {
					statement = "INSERT INTO " + tablename + " " +
						attributes + " VALUES " + values
				}
			} else {
				statement = "UPDATE " + tablename + " SET " + setUpdate +
					" WHERE " + cast.ToString(where)
			}

			if driverName == "hive" {
				e = sessionHive.Exec(statement, nil)
			} else {
				_, e = session.Exec(statement)
			}
			if e != nil {
				return errorlib.Error(packageName, modQuery+".Exec", commandType,
					cast.ToString(e.Error()))
			}

		} else if values == "" {
			return errorlib.Error(packageName, modQuery+".Exec", commandType,
				"please provide the data")
		}

	}
	if e != nil {
		return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
	}
	return nil
}
