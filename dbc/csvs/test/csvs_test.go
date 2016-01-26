package csvstest

import (
	"github.com/eaciit/dbox"
	_ "github.com/eaciit/dbox/dbc/csvs"
	"github.com/eaciit/toolkit"
	"os"

	// "strings"
	//"path/filepath"
	"testing"
)

var ctx dbox.IConnection

func connect() error {
	var e error
	if ctx == nil {
		wd, _ := os.Getwd()
		var config = map[string]interface{}{"useheader": true, "delimiter": ","}
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
	toolkit.Printf("List Table : %v\n", ctx.ObjectNames(dbox.ObjTypeTable))

	toolkit.Printf("All Object : %v\n", ctx.ObjectNames(""))
}

// func TestCRUD(t *testing.T) {
// 	skipIfConnectionIsNil(t)
// 	e := ctx.NewQuery().Delete().From(tableName).SetConfig("multiexec", true).Exec(nil)
// 	if e != nil {
// 		t.Fatalf("Delete fail: %s", e.Error())
// 	}

// 	es := []string{}
// 	qinsert := ctx.NewQuery().From(tableName).SetConfig("multiexec", true).Insert()
// 	for i := 1; i <= 50; i++ {
// 		u := &testUser{
// 			toolkit.Sprintf("user%d", i),
// 			toolkit.Sprintf("User %d", i),
// 			toolkit.RandInt(30) + 20, true}
// 		e = qinsert.Exec(toolkit.M{}.Set("data", u))
// 		if e != nil {
// 			es = append(es, toolkit.Sprintf("Insert fail %d: %s \n", i, e.Error()))
// 		}
// 	}

// 	if len(es) > 0 {
// 		t.Fatal(es)
// 	}

// 	e = ctx.NewQuery().Update().From(tableName).Where(dbox.Lte("_id", "user2")).Exec(toolkit.M{}.Set("data", toolkit.M{}.Set("Enable", false)))
// 	if e != nil {
// 		t.Fatalf("Update fail: %s", e.Error())
// 	}
// }

// func TestUpdate(t *testing.T) {
// 	skipIfConnectionIsNil(t)

// 	e := ctx.NewQuery().From(tableName).Save().Exec(toolkit.M{}.Set("data", toolkit.M{}.Set("_id", "user54").Set("Enable", false)))
// 	if e != nil {
// 		t.Fatalf("Specific update fail: %s", e.Error())
// 	}
// }

func TestSelect(t *testing.T) {
	skipIfConnectionIsNil(t)

	cursor, e := ctx.NewQuery().From(tableName).Where(dbox.Eq("Age", "34")).Cursor(nil)
	if e != nil {
		t.Fatalf("Cursor error: " + e.Error())
	}
	defer cursor.Close()

	// if cursor.Count() == 0 {
	// 	t.Fatalf("No record found")
	// }

	var datas []toolkit.M
	e = cursor.Fetch(&datas, 2, false)
	if e != nil {
		t.Fatalf("Fetch error: %s", e.Error())
	}
	// if len(datas) != cursor.Count() {
	// 	t.Fatalf("Expect %d records got %d\n%s\n", cursor.Count(), len(datas), toolkit.JsonString(datas))
	// }
	toolkit.Printf("Total Record : %d\n", cursor.Count())
	toolkit.Printf("Record found: %d\nData:\n%s\n", len(datas), toolkit.JsonString(datas))

	// toolkit.Printf("Record found: %d\nData:\n%s\n", len(datas),
	// 	func() string {
	// 		var ret []string
	// 		for _, v := range datas {
	// 			ret = append(ret, v.GetString("_id"))
	// 		}
	// 		return strings.Join(ret, ",")
	// 	}())
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
