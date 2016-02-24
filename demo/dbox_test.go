package dbox_test

import (
	"github.com/eaciit/dbox"
	_ "github.com/eaciit/dbox/dbc/jsons"
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
		ctx, e = dbox.NewConnection("jsons",
			&dbox.ConnectionInfo{wd, "", "", "", nil})
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
	// tableName string = "TestUsers"
	tableName string = "Orders"
)

type testUser struct {
	ID       string `json:"_id"`
	FullName string
	Age      int
	Enable   bool
}

type Orders struct {
	ID       string `json:"_id"`
	Nama     string `json:"nama"`
	Quantity int    `json:"quantity"`
	Price    int    `json:"price"`
	Amount   int    `json:"amount"`
	Status   string `json:"status"`
}

func TestFind(t *testing.T) {
	t.Skip()
	ms := []toolkit.M{}
	for i := 1; i <= 10; i++ {
		m := toolkit.M{}
		m.Set("_id", i)
		m.Set("random", toolkit.RandInt(100))
		ms = append(ms, m)
	}
	toolkit.Printf("Original Value\n%s\n", toolkit.JsonString(ms))

	indexes := dbox.Find(ms, []*dbox.Filter{
		//dbox.Or(dbox.Lt("random", 20), dbox.And(dbox.Gte("random", 60), dbox.Lte("random", 70)))})
		dbox.And(dbox.Gte("random", 30), dbox.Lte("random", 80))})

	records := []toolkit.M{}
	for _, v := range indexes {
		records = append(records, ms[v])
	}
	for _, r := range records {
		toolkit.Printf("Record: %s \n", toolkit.JsonString(r))
	}
	toolkit.Printf("Find %d records of %d records\n", len(indexes), len(ms))
	os.Exit(1)
}

func TestConnect(t *testing.T) {
	e := connect()
	if e != nil {
		t.Errorf("Error connecting to database: %s \n", e.Error())
	}
}

func TestCRUD(t *testing.T) {
	t.Skip()
	var e error
	skipIfConnectionIsNil(t)
	// e := ctx.NewQuery().Delete().From(tableName).SetConfig("multiexec", true).Exec(nil)
	// if e != nil {
	// 	t.Fatalf("Delete fail: %s", e.Error())
	// }

	es := []string{}
	qinsert := ctx.NewQuery().From(tableName).SetConfig("multiexec", true).Insert()
	for i := 1; i <= 50; i++ {
		u := &testUser{
			toolkit.Sprintf("user%d", i),
			toolkit.Sprintf("User %d", i),
			toolkit.RandInt(30) + 20, true}
		e = qinsert.Exec(toolkit.M{}.Set("data", u))
		if e != nil {
			es = append(es, toolkit.Sprintf("Insert fail %d: %s \n", i, e.Error()))
		}
	}

	if len(es) > 0 {
		t.Fatal(es)
	}

	// e = ctx.NewQuery().Update().From(tableName).Where(dbox.Lte("_id", "user2")).Exec(toolkit.M{}.Set("data", toolkit.M{}.Set("Enable", false)))
	// if e != nil {
	// 	t.Fatalf("Update fail: %s", e.Error())
	// }
}

func TestUpdate(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)

	e := ctx.NewQuery().From(tableName).Save().Exec(toolkit.M{}.Set("data", toolkit.M{}.Set("_id", "user54").Set("Enable", false)))
	if e != nil {
		t.Fatalf("Specific update fail: %s", e.Error())
	}
}

func TestSelect(t *testing.T) {
	// t.Skip()
	skipIfConnectionIsNil(t)

	cursor, e := ctx.NewQuery().
		Select("_id", "nama", "quantity", "price", "amount").
		From(tableName).
		// Where(dbox.Eq("nama", "buku")).
		// Where(dbox.Ne("nama", "buku")).
		// Where(dbox.Gt("price", 100000)).
		// Where(dbox.Gte("price", 100000)).
		// Where(dbox.Lt("price", 100000)).
		// Where(dbox.Lte("price", 100000)).
		// Where(dbox.In("nama", "tas", "dompet")).
		// Where(dbox.Nin("nama", "tas", "dompet")).
		// Where(dbox.And(dbox.Gt("amount", 100000), dbox.Eq("nama", "buku"))).
		Where(dbox.Contains("nama", "tem", "pe")).
		// Where(dbox.Or(dbox.Contains("nama", "bu"), dbox.Contains("nama", "do"))).
		// Where(dbox.Startwith("nama", "bu")).
		// Where(dbox.Endwith("nama", "as")).
		// Order("nama").
		// Skip(2).
		// Take(5).
		Cursor(nil)
	// Where(dbox.In("nama", "@name1", "@name2")).
	// Cursor(toolkit.M{}.Set("@name1", "stempel").Set("@name2", "buku"))
	// Where(dbox.Lte("price", "@price")).
	// Cursor(toolkit.M{}.Set("@price", 100000))
	// Where(dbox.Eq("nama", "@nama")).
	// Cursor(toolkit.M{}.Set("@nama", "tas"))
	// Where(dbox.Eq("price", "@price")).
	// Cursor(toolkit.M{}.Set("@price", 200000))
	// Where(dbox.And(dbox.Gt("price", "@price"), dbox.Eq("status", "@status"))).
	// Cursor(toolkit.M{}.Set("@price", 100000).Set("@status", "available"))
	// Where(dbox.And(dbox.Or(dbox.Eq("nama", "@name1"), dbox.Eq("nama", "@name2"),
	// dbox.Eq("nama", "@name3")), dbox.Lt("quantity", "@quantity"))).
	// Cursor(toolkit.M{}.Set("@name1", "buku").Set("@name2", "tas").
	// Set("@name3", "dompet").Set("@quantity", 4))
	// Where(dbox.Or(dbox.Or(dbox.Eq("nama", "@name1"), dbox.Eq("nama", "@name2"),
	// dbox.Eq("nama", "@name3")), dbox.Gt("quantity", "@quantity"))).
	// Cursor(toolkit.M{}.Set("@name1", "buku").Set("@name2", "tas").
	// Set("@name3", "dompet").Set("@quantity", 3))

	if e != nil {
		t.Fatalf("Cursor error: " + e.Error())
	}
	defer cursor.Close()

	if cursor.Count() == 0 {
		t.Fatalf("No record found")
	}

	var results []toolkit.M
	e = cursor.Fetch(&results, 0, false)

	if e != nil {
		t.Errorf("Unable to fetch: %s \n", e.Error())
	} else {
		toolkit.Println("======================")
		toolkit.Println("SELECT WITH FILTER")
		toolkit.Println("======================")
		toolkit.Println("Fetch OK. Result:")
		for _, val := range results {
			toolkit.Printf("%v \n",
				toolkit.JsonString(val))
		}
	}
	// if len(datas) != cursor.Count() {
	// 	t.Fatalf("Expect %d records got %d\n%s\n", cursor.Count(), len(datas), toolkit.JsonString(datas))
	// }
	// toolkit.Printf("Record found: %d\nData:\n%s\n", len(datas),
	// 	func() string {
	// 		var ret []string
	// 		for _, v := range datas {
	// 			ret = append(ret, v.GetString("_id"))
	// 		}
	// 		return strings.Join(ret, ",")
	// 	}())
}

func TestQueryAggregate(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)
	cursor, e := ctx.NewQuery().From(tableName).
		//Where(dbox.Lte("_id", "user600")).
		Aggr(dbox.AggrSum, 1, "Count").
		Aggr(dbox.AggrAvr, "$age", "AgeAverage").
		Group("enable").
		Cursor(nil)
	if e != nil {
		t.Fatalf("Unable to generate cursor. %s", e.Error())
	}
	defer cursor.Close()

	results := make([]toolkit.M, 0)
	e = cursor.Fetch(&results, 0, false)
	if e != nil {
		t.Errorf("Unable to iterate cursor %s", e.Error())
	} else {
		toolkit.Printf("Result:\n%s\n", toolkit.JsonString(results))
	}
}

func TestProcedure(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)
	inProc := toolkit.M{}.Set("name", "spSelectByFullName").Set("parms", toolkit.M{}.Set("@name", "User 20"))
	cursor, e := ctx.NewQuery().Command("procedure", inProc).Cursor(nil)
	if e != nil {
		t.Fatalf("Unable to generate cursor. %s", e.Error())
	}
	defer cursor.Close()

	results := make([]toolkit.M, 0)
	e = cursor.Fetch(&results, 0, false)
	if e != nil {
		t.Fatalf("Unable to iterate cursor %s", e.Error())
	} else if len(results) == 0 {
		t.Fatalf("No record returned")
	} else {
		toolkit.Printf("Result:\n%s\n", toolkit.JsonString(results[0:10]))
	}
}

func TestGetObj(t *testing.T) {
	toolkit.Printf("List Table : %v\n", ctx.ObjectNames(dbox.ObjTypeTable))

	toolkit.Printf("All Object : %v\n", ctx.ObjectNames(""))
}

func TestClose(t *testing.T) {
	skipIfConnectionIsNil(t)
	ctx.Close()
}
