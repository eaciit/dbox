package rdbms

import (
	"database/sql"
	//"encoding/json"
	"fmt"
	"github.com/eaciit/cast"
	"github.com/eaciit/crowd"
	//"github.com/eaciit/database/base"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
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
		//-- do nothing
	}
	return ret
}

func ReadVariable(f *dbox.Filter, in toolkit.M) *dbox.Filter {
	if (f.Op == "$and" || f.Op == "$or") &&
		strings.Contains(reflect.TypeOf(f.Value).String(), "dbox.Filter") {
		fs := f.Value.([]*dbox.Filter)
		// nilai fs :  [0xc082059590 0xc0820595c0]
		for i, ff := range fs {
			// nilai ff[0] : &{umur $gt @age} && ff[1] : &{name $eq @nama}
			bf := ReadVariable(ff, in)
			// nilai bf[0] :  &{umur $gt 25} && bf[1] : &{name $eq Kane}
			fs[i] = bf
		}
		f.Value = fs
		return f
	} else {
		if reflect.TypeOf(f.Value).Kind() == reflect.Slice {
			fSlice := f.Value.([]interface{})
			// nilai fSlice : [@name1 @name2]
			for i := 0; i < len(fSlice); i++ {
				// nilai fSlice [i] : @name1
				if string(cast.ToString(fSlice[i])[0]) == "@" {
					for key, val := range in {
						if strings.Replace(cast.ToString(fSlice[i]), "@", "", 1) == key {
							fSlice[i] = val
						}
					}
				}
			}
			f.Value = fSlice
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
	session := q.Session()
	cursor := dbox.NewCursor(new(Cursor))
	cursor.(*Cursor).session = session
	driverName := q.GetDriverDB()
	// driverName := "postgres"
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
				// isi qp :  &{AGGR {$sum 1 Total Item}}
				aggrInfo := qp.Value.(dbox.AggrInfo)
				// isi Aggr Info :  {$sum 1 Total Item}

				if incAtt == 0 {
					aggrExpression = strings.Replace(aggrInfo.Op, "$", "", 1) + "(" +
						cast.ToString(aggrInfo.Field) + ")" + " as '" + aggrInfo.Alias + "'"
				} else {
					aggrExpression += ", " + strings.Replace(aggrInfo.Op, "$", "", 1) +
						"(" + cast.ToString(aggrInfo.Field) + ")" + " as '" + aggrInfo.Alias + "'"
				}
				incAtt++
			}
			// isi Aggr Expression :  sum(1) as 'Total Item', max(amount) as 'Max Amount', avg(amount) as 'Average Amount'
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
			fmt.Println("nilai skip : ", cast.ToString(skip))
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
			// isi group expression :  GROUP BY nama
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

		cursor.(*Cursor).QueryString = QueryString

	} else if hasProcedure {
		procCommand := procedureParts.([]interface{})[0].(*dbox.QueryPart).Value.(interface{})
		fmt.Println("Isi Proc command : ", procCommand)

		spName := procCommand.(toolkit.M)["name"].(string) + " "
		params := procCommand.(toolkit.M)["parms"]
		incParam := 0
		ProcStatement := ""

		if driverName == "mysql" {
			paramValue := ""
			paramName := ""

			for key, val := range params.(toolkit.M) {
				if incParam == 0 {
					paramValue = "('" + val.(string) + "'"
					paramName = key
				} else {
					paramValue += ",'" + val.(string) + "'"
				}
				incParam += 1
			}
			paramValue += ", " + paramName + ")"

			ProcStatement = "CALL " + spName + paramValue
		} else if driverName == "mssql" {
			paramstring := ""
			incParam := 0
			for key, val := range params.(toolkit.M) {
				if incParam == 0 {
					paramstring = key + " = '" + val.(string) + "'"
				} else {
					paramstring += ", " + key + " = '" + val.(string) + "'"
				}
				incParam += 1
			}
			paramstring += ";"

			ProcStatement = "EXECUTE " + spName + paramstring
		} else if driverName == "oracle" {
			paramstring := ""
			incParam := 0
			for key, val := range params.(toolkit.M) {
				if incParam == 0 {
					if strings.Contains(key, "@@") {
						paramstring = "(" + strings.Replace(key, "@@", ":", 1)
					} else {
						paramstring = "('" + val.(string) + "'"
					}
				} else {
					if strings.Contains(key, "@@") {
						paramstring += "," + strings.Replace(key, "@@", ":", 1)
					} else {
						paramstring += ",'" + val.(string) + "'"
					}
				}
				incParam += 1
			}
			paramstring += ");"

			ProcStatement = "EXECUTE " + spName + paramstring

		} else if driverName == "postgres" {
			paramValue := ""
			incParam := 0
			for key, val := range params.(toolkit.M) {
				fmt.Println("===============", val)
				if val != "" {
					if incParam == 0 {
						fmt.Println("++++++++++ param value 0")
						paramValue = "('" + val.(string) + "'"
						//paramName = key
						fmt.Println(key)
					} else {
						paramValue += ",'" + val.(string) + "'"
					}
					incParam += 1
				} else {
					paramValue = "( "
				}
			}
			paramValue += ");"

			ProcStatement = "SELECT * FROM " + spName + paramValue
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
	// fmt.Println("Parameter Exec : ", parm)

	dbname := q.Connection().Info().Database
	tablename := ""

	if parm == nil {
		parm = toolkit.M{}
	}
	data := parm.Get("data", nil)

	// fmt.Println("Hasil ekstraksi Param : ", data)

	//========================EXTRACT FIELD, DATA AND FORMAT DATE=============================

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
			stringValues := StringValue(dataValues, q.GetDriverDB())
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

	//=================================END OF EXTRACTION=======================================

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

	if data == nil {
		//---
		multi = true
	} else {
		if where == nil {
			id := toolkit.Id(data)
			if id != nil {
				where = (toolkit.M{}).Set("_id", id)
			}
		} else {
			multi = true
		}
	}
	session := q.Session()

	if dbname != "" && tablename != "" && multi == true {

	}
	if commandType == dbox.QueryPartInsert {

	} else if commandType == dbox.QueryPartUpdate {
		statement := "UPDATE " + tablename + " SET " + setUpdate + " WHERE " + cast.ToString(where)
		fmt.Println("Update Statement : ", statement)
		_, e = session.Exec(statement)
		if e != nil {
			fmt.Println(e.Error())
		}

	} else if commandType == dbox.QueryPartDelete {
		if where == nil {
			statement := "DELETE FROM " + tablename
			fmt.Println(statement)
			_, e = session.Exec(statement)
			if e != nil {
				fmt.Println(e.Error())
			}
		} else {
			statement := "DELETE FROM " + tablename + " where " + cast.ToString(where)
			fmt.Println(statement)
			_, e = session.Exec(statement)
			if e != nil {
				fmt.Println(e.Error())
			}
		}

	} else if commandType == dbox.QueryPartSave {

		statement := "INSERT INTO " + tablename + " " + attributes + " VALUES " + values
		fmt.Println("Insert Statement : ", statement)
		_, e = session.Exec(statement)
		if e != nil {
			fmt.Println(e.Error())
		}
	}
	if e != nil {
		return errorlib.Error(packageName, modQuery+".Exec", commandType, e.Error())
	}
	return nil
}
