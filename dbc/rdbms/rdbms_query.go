

package rdbms

import (
	"database/sql"
	"encoding/json"
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
	Sql            sql.DB
	usePooling     bool
	DriverDB       string
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

func (q *Query) Cursor(in toolkit.M) (dbox.ICursor, error) {
	var e error
	/*
		if q.Parts == nil {
			return nil, errorlib.Error(packageName, modQuery,
				"Cursor", fmt.Sprintf("No Query Parts"))
		}
	*/

	aggregate := false
	dbname := q.Connection().Info().Database
	tablename := ""

	/*
		parts will return E - map{interface{}}interface{}
		where each interface{} returned is slice of interfaces --> []interface{}
	*/
	parts := crowd.From(q.Parts()).Group(func(x interface{}) interface{} {
		qp := x.(*dbox.QueryPart)
		return qp.PartType
	}, nil).Data

	fromParts, hasFrom := parts[dbox.QueryPartFrom]
	if hasFrom == false {
		return nil, errorlib.Error(packageName, "Query", "Cursor", "Invalid table name")
	}
	tablename = fromParts.([]interface{})[0].(*dbox.QueryPart).Value.(string)

	skip := 0
	if skipParts, hasSkip := parts[dbox.QueryPartSkip]; hasSkip {
		skip = skipParts.([]interface{})[0].(*dbox.QueryPart).
			Value.(int)
	}

	take := 0
	if takeParts, has := parts[dbox.QueryPartTake]; has {
		take = takeParts.([]interface{})[0].(*dbox.QueryPart).
			Value.(int)
	}

	var fields toolkit.M
	selectParts, hasSelect := parts[dbox.QueryPartSelect]
	var attribute string
	incAtt := 0
	if hasSelect {
		fields = toolkit.M{}
		for _, sl := range selectParts.([]interface{}) {
			qp := sl.(*dbox.QueryPart)
			for _, fid := range qp.Value.([]string) {
				if incAtt == 0 {
					attribute = fid
				} else {
					attribute = attribute + "," + fid
				}
				incAtt++
				fields.Set(fid, 1)
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
	//fmt.Printf("Result: %s \n", toolkit.JsonString(fields))
	//fmt.Printf("Database:%s table:%s \n", dbname, tablename)
	var sort []string
	sortParts, hasSort := parts[dbox.QueryPartSelect]
	if hasSort {
		sort = []string{}
		for _, sl := range sortParts.([]interface{}) {
			qp := sl.(*dbox.QueryPart)
			for _, fid := range qp.Value.([]string) {
				sort = append(sort, fid)
			}
		}
	}

	//where := toolkit.M{}
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
			return nil, errorlib.Error(packageName, modQuery, "Cursor",
				e.Error())
		} else {
			//fmt.Printf("Where: %s", toolkit.JsonString(where))
		}
		//where = iwhere.(toolkit.M)
	}

	session := q.Session()
	cursor := dbox.NewCursor(new(Cursor))
	cursor.(*Cursor).session = session
	if dbname != "" && tablename != "" && e != nil && skip == 0 && take == 0 && where == nil {

	}
	if !aggregate {
		QueryString := ""
		if attribute == "" {
			QueryString = "SELECT * FROM " + tablename
		} else {
			QueryString = "SELECT " + attribute + " FROM " + tablename
		}
		if cast.ToString(where) != "" {
			QueryString = QueryString + " WHERE " + cast.ToString(where)
		}
		cursor.(*Cursor).QueryString = QueryString
	} else {

	}
	return cursor, nil
}

func  StringValue(v interface{},db string) string {
	var ret string
	switch v.(type) {
	case string:
		ret = fmt.Sprintf("%s","'"+ v.(string)+"'")
	case time.Time:
		t := v.(time.Time).UTC() 
		if(strings.Contains(db,"oracle")){  
			ret = "to_date('"+t.Format("2006-01-02 15:04:05")+"','yyyy-mm-dd hh24:mi:ss')" 	
		}else{ 
			ret = "'"+t.Format("2006-01-02 15:04:05")+"'" 
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
	var tanya string
	var values string //[]interface{}{}
   if(data!=nil){
		var reflectValue = reflect.ValueOf(data)
		if  reflectValue.Kind() == reflect.Ptr {
			reflectValue = reflectValue.Elem()
		}
		var reflectType = reflectValue.Type() 

		for i := 0; i < reflectValue.NumField(); i++ {
			namaField := reflectType.Field(i).Name
			//tipeData := reflectType.Field(i).Type
			dataValues := reflectValue.Field(i).Interface()
			if i == 0 {
				attributes = "(" + namaField
				tanya = "(" + "?"
				values = StringValue(dataValues,q.GetDriverDB())

			} else {
				attributes = attributes + "," + namaField
				tanya = tanya + "," + "?"
				values = values+","+StringValue(dataValues,q.GetDriverDB())

			}
			// values = append(values, StringValue(dataValues))

		}
	}
	//=================================END OF EXTRACTION=======================================

	 
	temp := ""
	parts := crowd.From(q.Parts()).Group(func(x interface{}) interface{} {
		qp := x.(*dbox.QueryPart)
		// fmt.Printf("[%s] QP = %s \n",
		// 	toolkit.Id(data),
		// 	toolkit.JsonString(qp))
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
	row, _ := json.Marshal(data)
	result := string(row) 
	result = strings.Replace(result, "{", "", -1)
	result = strings.Replace(result, "}", "", -1)
	result = strings.Replace(result, "\\", "", -1) 

	if dbname != "" && tablename != "" && multi == true {

	}
	if commandType == dbox.QueryPartInsert {
	 
	} else if commandType == dbox.QueryPartUpdate {
		result := strings.Replace(result, ":", "=", -1)
		datas := strings.Split(result, ",")
		var attribute []string
		var set string
		var vals []string
		for i := 0; i < len(datas); i++ {
			rows := strings.Split(datas[i], "=")
			for j := 0; j < len(rows); j++ {
				if j == 0 {
					rows[j] = strings.Replace(rows[j], "\"", "", -1)
					attribute = append(attribute, rows[j])
				} else {
					vals = append(vals, rows[j])
				}
			}
		}
		for i := 0; i < len(attribute); i++ {
			if i == 0 {
				vals[i] = strings.Replace(vals[i], "\"", "'", -1)
				set = set + attribute[i] + "=" + vals[i]
			} else {
				vals[i] = strings.Replace(vals[i], "\"", "'", -1)
				set = set + "," + attribute[i] + "=" + vals[i]
			}
		}
		statement := "UPDATE " + tablename + " SET " + set + " WHERE " + cast.ToString(where)
		fmt.Println(statement)
		_, e = session.Exec(statement)
		if e != nil {
			fmt.Println(e.Error())
		}
	} else if commandType == dbox.QueryPartDelete {
		if where == nil {
			statement :="DELETE FROM "+ tablename
			fmt.Println(statement)
			_, e = session.Exec(statement)
			if e != nil {
				fmt.Println(e.Error())
			}
		} else {
			statement :="DELETE FROM " + tablename + " where " + cast.ToString(where)
			fmt.Println(statement)
			_, e = session.Exec(statement)
			if e != nil {
				fmt.Println(e.Error())
			}
		}

	} else if commandType == dbox.QueryPartSave {
		attributes = attributes + ")"
		tanya = tanya + ")"
		
		// sqlStr := "INSERT INTO " + tablename + " " + attributes + " VALUES " + tanya
		// val :=""
		// for i := 0; i < len(values); i++ {
		// 	if(i==0){
		// 		val="("+cast.ToString(values[i])
		// 	}else{
		// 		val=val+","+cast.ToString(values[i])
		// 	}
		// }

		// val=val+")"
		statement := "INSERT INTO " + tablename + " " + attributes + " VALUES ( "+values+ " )"		
		// fmt.Println("Syntax Insert : ", sqlStr)
		// fmt.Println("Data yang akan di insert : ", values)
		// stmt, _ := session.Prepare(sqlStr)
		// fmt.Println("Session data : ", stmt)

		// res, e := stmt.Exec(values...)
		// if e != nil {
		// 	fmt.Println(res)
		// }
		// fmt.Println(statement)
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
