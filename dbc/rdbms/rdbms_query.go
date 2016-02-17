package rdbms

import (
	"database/sql"
	//"encoding/json"
	"fmt"
	"github.com/eaciit/cast"
	"github.com/eaciit/crowd"
<<<<<<< HEAD
=======
	"github.com/ranggadablues/dbox"
>>>>>>> bbe204ed9e388ba424883a5ce94877c03ef0bba5
	"github.com/eaciit/errorlib"
	"github.com/eaciit/hdc/hive"
	"github.com/eaciit/toolkit"
	"github.com/rinosukmandityo/dbox"
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
	// if q.Sql != nil && q.usePooling == false {
	q.Sql.Close()
	// }
}

func (q *Query) Prepare() error {
	return nil
}

func StringValue(v interface{}, db string) string {
	var ret string
	switch v.(type) {
	case string:
		ret = fmt.Sprintf("%s", "'"+v.(string)+"'")
	case time.Time:
		t := v.(time.Time).UTC()
		if strings.Contains(db, "oracle") {
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
							if strings.Replace(cast.ToString(fSlice[i]), "@", "", 1) == key {
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
							if strings.Replace(fSlice[i], "@", "", 1) == key {
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
					if strings.Replace(cast.ToString(f.Value), "@", "", 1) == key {
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
	driverName := q.GetDriverDB()
	// driverName = "oracle"
	var QueryString string

	/*
		parts will return E - map{interface{}}interface{}
		where each interface{} returned is slice of interfaces --> []interface{}
	*/
	parts := crowd.From(q.Parts()).Group(func(x interface{}) interface{} {
		qp := x.(*dbox.QueryPart)
		return qp.PartType
	}, nil).Data

	fromParts, hasFrom := parts[dbox.QueryPartFrom]
	procedureParts, hasProcedure := parts["procedure"]

	if hasFrom {
		tablename := ""
		tablename = fromParts.([]interface{})[0].(*dbox.QueryPart).Value.(string)

		selectParts, hasSelect := parts[dbox.QueryPartSelect]
		var attribute string
		incAtt := 0
		if hasSelect {
			for _, sl := range selectParts.([]interface{}) {
				qp := sl.(*dbox.QueryPart)
				for _, fid := range qp.Value.([]string) {
					if incAtt == 0 {
						attribute = fid
					} else {
						attribute = attribute + ", " + fid
					}
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
			for _, aggr := range aggrParts.([]interface{}) {
				qp := aggr.(*dbox.QueryPart)
				/* isi qp :  &{AGGR {$sum 1 Total Item}}*/
				aggrInfo := qp.Value.(dbox.AggrInfo)
				/* isi Aggr Info :  {$sum 1 Total Item}*/

				if incAtt == 0 {
					aggrExpression = strings.Replace(aggrInfo.Op, "$", "", 1) + "(" +
						cast.ToString(aggrInfo.Field) + ")" + " as \"" + aggrInfo.Alias + "\""
				} else {
					aggrExpression += ", " + strings.Replace(aggrInfo.Op, "$", "", 1) +
						"(" + cast.ToString(aggrInfo.Field) + ")" + " as \"" + aggrInfo.Alias + "\""
				}
				incAtt++
			}
			/* isi Aggr Expression :  sum(1) as 'Total Item', max(amount) as 'Max Amount', avg(amount) as 'Average Amount'*/
		}

		var where interface{}
		whereParts, hasWhere := parts[dbox.QueryPartWhere]
		if hasWhere {
			fb := q.Connection().Fb()
			for _, p := range whereParts.([]interface{}) {
				fs := p.(*dbox.QueryPart).Value.([]*dbox.Filter)
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
			for _, oval := range orderParts.([]interface{}) {
				qp := oval.(*dbox.QueryPart)
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
			}
		}

		skip := 0
		skipParts, hasSkip := parts[dbox.QueryPartSkip]
		if hasSkip {
			skip = skipParts.([]interface{})[0].(*dbox.QueryPart).
				Value.(int)
		}

		take := 0
		takeParts, hasTake := parts[dbox.QueryPartTake]
		if hasTake {
			take = takeParts.([]interface{})[0].(*dbox.QueryPart).
				Value.(int)
		}

		partGroup, hasGroup := parts[dbox.QueryPartGroup]
		var groupExpression string
		if hasGroup {
			for _, aggr := range partGroup.([]interface{}) {
				qp := aggr.(*dbox.QueryPart)
				groupValue := qp.Value.([]string)
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
				QueryString = "SELECT " + attribute + " FROM " + tablename
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

		if driverName == "mysql" || driverName == "hive" {
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

		} else if driverName == "oracle" {
			if hasSkip && hasTake {
				QueryString += " ROWNUM <= " + cast.ToString(take) + " OFFSET " + cast.ToString(skip)
			} else if hasSkip && !hasTake {

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
		}
		// fmt.Println("query string : ", QueryString)
		cursor.(*Cursor).QueryString = QueryString

	} else if hasProcedure {
		procCommand := procedureParts.([]interface{})[0].(*dbox.QueryPart).Value.(interface{})
		fmt.Println("Isi Proc command : ", procCommand)

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

					fmt.Println("Print value order", paramstring)
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
		} else if driverName == "oracle" {
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

					fmt.Println("Print value order", paramstring)
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

		fmt.Println("Proc Statement : ", ProcStatement)
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

	if parm == nil {
		parm = toolkit.M{}
	}
	data := parm.Get("data", nil)
	// fmt.Println("Hasil ekstraksi Param : ", data)

	/*========================EXTRACT FIELD, DATA AND FORMAT DATE=============================*/

	var attributes string
	var values string
	var setUpdate string

	if data != nil {

		var reflectValue = reflect.ValueOf(data)
		if reflectValue.Kind() == reflect.Ptr {
			reflectValue = reflectValue.Elem()
		}
		var reflectType = reflectValue.Type()

		for i := 0; i < reflectValue.NumField(); i++ {
			namaField := reflectType.Field(i).Name
			dataValues := reflectValue.Field(i).Interface()
			stringValues := StringValue(dataValues, driverName)
			if i == 0 {
				attributes = "(" + namaField
				values = "(" + stringValues
				setUpdate = namaField + " = " + stringValues
			} else {
				attributes += ", " + namaField
				values += ", " + stringValues
				setUpdate += ", " + namaField + " = " + stringValues
			}
		}
		attributes += ")"
		values += ")"
	}

	/*=================================END OF EXTRACTION=======================================*/

	temp := ""
	parts := crowd.From(q.Parts()).Group(func(x interface{}) interface{} {
		qp := x.(*dbox.QueryPart)
		temp = toolkit.JsonString(qp)
		return qp.PartType
	}, nil).Data

	fromParts, hasFrom := parts[dbox.QueryPartFrom]
	if !hasFrom {

		return errorlib.Error(packageName, "Query", modQuery, "Invalid table name")
	}
	tablename = fromParts.([]interface{})[0].(*dbox.QueryPart).Value.(string)

	var where interface{}
	whereParts, hasWhere := parts[dbox.QueryPartWhere]
	if hasWhere {
		fb := q.Connection().Fb()
		for _, p := range whereParts.([]interface{}) {
			fs := p.(*dbox.QueryPart).Value.([]*dbox.Filter)
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
	multiExec := q.Config("multiexec", false).(bool)

	if dbname != "" && tablename != "" && multi == true {

	}
	if commandType == dbox.QueryPartInsert {
		if attributes != "" && values != "" {
			statement := "INSERT INTO " + tablename + " " + attributes + " VALUES " + values
			fmt.Println("Insert Statement : ", statement)
			_, e = session.Exec(statement)
			if e != nil {
				fmt.Println(e.Error())
			}
		} else {
			return errorlib.Error(packageName, modQuery+".Exec", commandType,
				"please provide the data")
		}

	} else if commandType == dbox.QueryPartUpdate {
		if setUpdate != "" {
			var querystmt string
			if where != nil {
				querystmt = "select count(*) from " + tablename +
					" where " + cast.ToString(where)
			} else {
				querystmt = "select count(*) from " + tablename
			}

			rows, _ := session.Query(querystmt)
			var rowCount int
			for rows.Next() {
				rows.Scan(&rowCount)
			}

			if rowCount == 0 {
				return errorlib.Error(packageName, modQuery+".Exec", commandType,
					"can't find any related record")
			} else if rowCount == 1 || (rowCount > 1 && multiExec) {
				var statement string
				if where != nil {
					statement = "UPDATE " + tablename + " SET " + setUpdate +
						" WHERE " + cast.ToString(where)
				} else {
					statement = "UPDATE " + tablename + " SET " + setUpdate
				}
				fmt.Println("Update Statement : ", statement)
				_, e = session.Exec(statement)
				if e != nil {
					return errorlib.Error(packageName, modQuery+".Exec", commandType,
						cast.ToString(e.Error()))
				}
			} else {
				return errorlib.Error(packageName, modQuery+".Exec", commandType,
					"please use multiexec to update more than one row")
			}
		} else if setUpdate == "" {
			return errorlib.Error(packageName, modQuery+".Exec", commandType,
				"please provide the data")
		}

	} else if commandType == dbox.QueryPartDelete {
		var querystmt string
		if where != nil {
			querystmt = "select count(*) from " + tablename +
				" where " + cast.ToString(where)
		} else {
			querystmt = "select count(*) from " + tablename
		}

		rows, _ := session.Query(querystmt)
		var rowCount int
		for rows.Next() {
			rows.Scan(&rowCount)
		}

		if rowCount == 0 {
			return errorlib.Error(packageName, modQuery+".Exec", commandType,
				"can't find any related record")
		} else if rowCount == 1 || (rowCount > 1 && multiExec) {
			var statement string
			if where != nil {
				statement = "DELETE FROM " + tablename + " where " + cast.ToString(where)
			} else {
				statement = "DELETE FROM " + tablename
			}
			fmt.Println("Delete Statement : ", statement)
			_, e = session.Exec(statement)
			if e != nil {
				fmt.Println(e.Error())
			}
		} else if rowCount > 1 && !multiExec {
			return errorlib.Error(packageName, modQuery+".Exec", commandType,
				"please use multiexec to delete more than one row")
		}
	} else if commandType == dbox.QueryPartSave {
		if attributes != "" && values != "" {
			var querystmt string
			if where != nil {
				querystmt = "select count(*) from " + tablename +
					" where " + cast.ToString(where)
			}
			rows, _ := session.Query(querystmt)
			var rowCount int
			for rows.Next() {
				rows.Scan(&rowCount)
			}

			var statement string
			if rowCount == 0 || where == nil {
				statement = "INSERT INTO " + tablename + " " +
					attributes + " VALUES " + values
			} else if rowCount == 1 || (rowCount > 1 && multiExec) {
				statement = "UPDATE " + tablename + " SET " + setUpdate +
					" WHERE " + cast.ToString(where)
			} else {
				return errorlib.Error(packageName, modQuery+".Exec", commandType,
					"please use multiexec to save more than one row")
			}

			fmt.Println("Save Statement : ", statement)
			_, e = session.Exec(statement)
			if e != nil {
				fmt.Println(e.Error())
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
