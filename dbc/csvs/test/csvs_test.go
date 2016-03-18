package csvstest

import (
	"github.com/eaciit/dbox"
	_ "github.com/eaciit/dbox/dbc/csvs"
	"github.com/eaciit/toolkit"
	"os"

	// "strings"
	//"path/filepath"
	"fmt"
	"testing"
)

type employee struct {
	Id        string
	FirstName string
	LastName  string
	Age       int
}

var ctx dbox.IConnection

func connect() error {
	var e error
	if ctx == nil {
		wd, _ := os.Getwd()
		var config = map[string]interface{}{"useheader": true, "delimiter": ",", "newfile": true}
		ctx, e = dbox.NewConnection("csvs",
			&dbox.ConnectionInfo{wd, "", "", "", config})
		if e != nil {
			return e
		}
	}
	e = ctx.Connect()
	return e
}

func close() {
	if ctx != nil {
		ctx.Close()
	}
}

func skipIfConnectionIsNil(t *testing.T) {
	if ctx == nil {
		t.Skip()
	}
}

const (
	tableName string = "Data_Comma"
)

type testUser struct {
	EmployeeId string
	FirstName  string
	LastName   string
	Age        int
	JoinDate   string
	Email      string
	Phone      string
}

func TestConnect(t *testing.T) {
	e := connect()
	if e != nil {
		t.Errorf("Error connecting to database: %s \n", e.Error())
	}
}

func TestGetObj(t *testing.T) {
	t.Skip("Skip : Comment this line to do test")
	toolkit.Printf("List Table : %v\n", ctx.ObjectNames(dbox.ObjTypeTable))
	toolkit.Printf("All Object : %v\n", ctx.ObjectNames(""))
}

func TestInsert(t *testing.T) {
	// t.Skip("Skip : Comment this line to do test")
	skipIfConnectionIsNil(t)

	es := []string{}
	qinsert := ctx.NewQuery().From("Data_CUD").SetConfig("multiexec", true).Save()

	for i := 1; i <= 5; i++ {
		u := toolkit.M{}.Set("Id", toolkit.Sprintf("ID-1%d", i)).
			Set("Email", toolkit.Sprintf("user-1%d", i)).
			Set("FirstName", toolkit.Sprintf("User no.%d", i)).
			Set("LastName", toolkit.Sprintf("Test no.%d", i))

		e := qinsert.Exec(toolkit.M{}.Set("data", u))
		if e != nil {
			es = append(es, toolkit.Sprintf("Insert fail %d: %s \n", i, e.Error()))
		}
	}

	if len(es) > 0 {
		t.Fatal(es)
	}
}

func TestUpdate(t *testing.T) {
	t.Skip("Skip : Comment this line to do test")
	skipIfConnectionIsNil(t)

	e := ctx.NewQuery().Update().From("Data_CUD").Where(dbox.Eq("Id", "ID-11")).Exec(toolkit.M{}.Set("data", toolkit.M{}.Set("Phone", "0874-XXX-CCC")))
	if e != nil {
		t.Fatalf("Update fail: %s", e.Error())
	}
}

func TestSave(t *testing.T) {
	t.Skip("Skip : Comment this line to do test")
	skipIfConnectionIsNil(t)

	e := ctx.NewQuery().From("Data_CUD").Save().Exec(toolkit.M{}.Set("data", toolkit.M{}.Set("Id", "ID-1").Set("Phone", "XXX-0856-244").Set("JoinDate", "2014-11-01")))
	if e != nil {
		t.Fatalf("Specific update fail: %s", e.Error())
	}

	e = ctx.NewQuery().From("Data_CUD").Save().Exec(toolkit.M{}.Set("data", toolkit.M{}.
		Set("Id", "ID-11").
		Set("Email", "user123@yahoo.com").
		Set("FirstName", "Test 123").
		Set("lastname", "rr").
		Set("Phone", "XXX-0852").
		Set("JoinDate", "2014-11-03")))
	if e != nil {
		t.Fatalf("Specific update fail: %s", e.Error())
	}
}

func TestDelete(t *testing.T) {
	t.Skip("Skip : Comment this line to do test")
	skipIfConnectionIsNil(t)
	e := ctx.NewQuery().Delete().From("Data_CUD").SetConfig("multiexec", true).Where(dbox.Or(dbox.Eq("Id", "ID-11"), dbox.Eq("Id", "ID-13"))).Exec(nil)
	if e != nil {
		t.Fatalf("Delete fail: %s", e.Error())
	}
}

func TestSelect(t *testing.T) {
	t.Skip("Skip : Comment this line to do test")
	skipIfConnectionIsNil(t)

	cursor, e := ctx.NewQuery().From(tableName).Where(dbox.Eq("Age", 34)).Cursor(nil)
	if e != nil {
		t.Fatalf("Cursor error: " + e.Error())
	}
	defer cursor.Close()

	var datas []toolkit.M
	e = cursor.Fetch(&datas, 2, false)
	if e != nil {
		t.Fatalf("Fetch error: %s", e.Error())
	}

	toolkit.Printf("Total Record : %d\n", cursor.Count())
	toolkit.Printf("Record found: %d\nData:\n%s\n", len(datas), toolkit.JsonString(datas))
}

func TestSelectLimit(t *testing.T) {
	t.Skip("Skip : Comment this line to do test")
	skipIfConnectionIsNil(t)

	cursor, e := ctx.NewQuery().
		Select("EmployeeId", "FirstName").
		Skip(0).Take(3).
		From(tableName).
		Cursor(nil)

	if e != nil {
		t.Fatalf("Cursor error: " + e.Error())
	}

	datas := make([]toolkit.M, 0, 0)
	e = cursor.Fetch(&datas, 0, false)
	if e != nil {
		t.Fatalf("Fetch error: %s", e.Error())
	}

	toolkit.Printf("Total Record : %d\n", cursor.Count())
	toolkit.Printf("Record found: %d\nData:\n%s\n", len(datas), toolkit.JsonString(datas))

	cursor.Close()

	//==================
	cursor, e = ctx.NewQuery().
		Select("EmployeeId").
		Skip(3).Take(5).
		From(tableName).
		Cursor(nil)

	if e != nil {
		t.Fatalf("Cursor error: " + e.Error())
	}

	datas = make([]toolkit.M, 0, 0)
	e = cursor.Fetch(&datas, 0, false)
	if e != nil {
		t.Fatalf("Fetch error: %s", e.Error())
	}

	toolkit.Printf("Total Record : %d\n", cursor.Count())
	toolkit.Printf("Record found: %d\nData:\n%s\n", len(datas), toolkit.JsonString(datas))

	cursor.Close()
}

func TestSelectCondition(t *testing.T) {
	t.Skip("Just Skip Test")
	skipIfConnectionIsNil(t)

	// cursor, e := ctx.NewQuery().
	// c, e := prepareConnection()
	// if e != nil {
	// 	t.Errorf("Unable to connect %s \n", e.Error())
	// }
	// defer c.Close()

	csr, e := ctx.NewQuery().Select("Id", "LastName", "Age").
		Where(dbox.Contains("LastName", "m")).
		From("BackData01").
		Take(10).Skip(0).
		Cursor(nil)
	if e != nil {
		t.Errorf("Cursor pre error: %s \n", e.Error())
		return
	}

	if csr == nil {
		t.Errorf("Cursor not initialized")
		return
	}

	resultsstruct := make([]employee, 0)
	e = csr.Fetch(&resultsstruct, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch N(0-10): %s \n", e.Error())
	} else {
		fmt.Printf("Record count(0-10) : %v \n", csr.Count())
		fmt.Printf("Fetch N(0-10) OK. Result: %v \n", resultsstruct)
	}

	csr.Close()

	csr, e = ctx.NewQuery().Select("Id", "LastName", "Age").
		Where(dbox.Contains("LastName", "m")).
		From("BackData01").
		Take(10).Skip(10).
		Cursor(nil)
	if e != nil {
		t.Errorf("Cursor pre error: %s \n", e.Error())
		return
	}

	if csr == nil {
		t.Errorf("Cursor not initialized")
		return
	}

	resultsstruct = make([]employee, 0)
	e = csr.Fetch(&resultsstruct, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch N(10-20): %s \n", e.Error())
	} else {
		fmt.Printf("Record count(10-20) : %v \n", csr.Count())
		fmt.Printf("Fetch N(10-20) OK. Result: %v \n", resultsstruct)
	}

	csr.Close()

}

// func TestQueryAggregate(t *testing.T) {
// 	t.Skip()
// 	skipIfConnectionIsNil(t)
// 	cursor, e := ctx.NewQuery().From(tableName).
// 		//Where(dbox.Lte("_id", "user600")).
// 		Aggr(dbox.AggrSum, 1, "Count").
// 		Aggr(dbox.AggrAvr, "$age", "AgeAverage").
// 		Group("enable").
// 		Cursor(nil)
// 	if e != nil {
// 		t.Fatalf("Unable to generate cursor. %s", e.Error())
// 	}
// 	defer cursor.Close()

// 	results := make([]toolkit.M, 0)
// 	e = cursor.Fetch(&results, 0, false)
// 	if e != nil {
// 		t.Errorf("Unable to iterate cursor %s", e.Error())
// 	} else {
// 		toolkit.Printf("Result:\n%s\n", toolkit.JsonString(results))
// 	}
// }

// func TestProcedure(t *testing.T) {
// 	t.Skip()
// 	skipIfConnectionIsNil(t)
// 	inProc := toolkit.M{}.Set("name", "spSelectByFullName").Set("parms", toolkit.M{}.Set("@name", "User 20"))
// 	cursor, e := ctx.NewQuery().Command("procedure", inProc).Cursor(nil)
// 	if e != nil {
// 		t.Fatalf("Unable to generate cursor. %s", e.Error())
// 	}
// 	defer cursor.Close()

// 	results := make([]toolkit.M, 0)
// 	e = cursor.Fetch(&results, 0, false)
// 	if e != nil {
// 		t.Fatalf("Unable to iterate cursor %s", e.Error())
// 	} else if len(results) == 0 {
// 		t.Fatalf("No record returned")
// 	} else {
// 		toolkit.Printf("Result:\n%s\n", toolkit.JsonString(results[0:10]))
// 	}
// }

func TestClose(t *testing.T) {
	skipIfConnectionIsNil(t)
	ctx.Close()
}
