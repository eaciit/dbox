package rdbms

import (
	"errors"
	"fmt"
	"github.com/eaciit/dbox"
	// errors "github.com/eaciit/errorlib"
	_ "github.com/eaciit/toolkit"
	"database/sql"
	//"reflect"
)

const (
	modCursor = "Cursor"

	QueryResultCursor = "SQLCursor"
	QueryResultPipe   = "SQLPipe"
)

type Cursor struct {
	dbox.Cursor
	ResultType string
	count int
	start int 
	session    		sql.DB
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
	c.start=0
	return nil
}

func (c *Cursor) Fetch(m interface{}, n int, closeWhenDone bool) (
	*dbox.DataSet, error) {
	ds := dbox.NewDataSet(m)
	fmt.Println(c.QueryString)
	rows , e := c.session.Query(c.QueryString)
	if e != nil {
	   return nil, e
	}
	defer rows.Close()
	columns, e := rows.Columns()
	if e != nil {
	   return nil, e
	}
	count := len(columns)
	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	for rows.Next() {
	    for i := 0; i < count; i++ {
	    valuePtrs[i] = &values[i]
	}
	rows.Scan(valuePtrs...)
	entry := make(map[string]interface{})
	for i, col := range columns {
	    var v interface{}
	    val := values[i]
	    b, ok := val.([]byte)
	    if ok {
	        v = string(b)
	    } else {
	        v = val
	    }
	entry[col] = v
	}
		tableData = append(tableData, entry)
	}
	if e != nil {
	    return nil, e
	}
	if(n==0){
		for i := 0; i < len(tableData); i++ {
			ds.Data = append(ds.Data, tableData[i])
		}	
	}else{  
		end:= c.start+n 
		if(end > len(tableData)){
			e = errors.New("index out of range")
		}else{
			for i := c.start ; i < end ; i++ { 
				ds.Data = append(ds.Data, tableData[i])
				c.start=i+1
			}
			e = nil
		}
	}

	
	return ds,e
}
 