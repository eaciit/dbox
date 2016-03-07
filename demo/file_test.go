package flat_demo

import (
	"github.com/eaciit/dbox"
	_ "github.com/eaciit/dbox/dbc/jsons"
	"github.com/eaciit/toolkit"
	"os"
	"testing"
)

var ctx dbox.IConnection
var sintaks string = `
		ctx.NewQuery().
		Select("_id", "nama", "amount").
		From(tableName).
		Cursor(nil)`
var operation string = "TestSelect"

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
	tableName string = "Orders"
)

type Orders struct {
	ID       string `json:"_id"`
	Nama     string `json:"nama"`
	Quantity int    `json:"quantity"`
	Price    int    `json:"price"`
	Amount   int    `json:"amount"`
	Status   string `json:"status"`
}

func TestConnect(t *testing.T) {
	e := connect()
	if e != nil {
		t.Errorf("Error connecting to database: %s \n", e.Error())
	}
}

// func TestSelect(t *testing.T) {
// 	t.Skip()
// 	skipIfConnectionIsNil(t)

// 	cursor, e := ctx.NewQuery().
// 		Select("_id", "nama", "amount").
// 		From(tableName).
// 		Cursor(nil)

// 	if e != nil {
// 		t.Fatalf("Cursor error: " + e.Error())
// 	}
// 	defer cursor.Close()

// 	if cursor.Count() == 0 {
// 		t.Fatalf("No record found")
// 	}

// 	var results []toolkit.M
// 	e = cursor.Fetch(&results, 0, false)

// 	if e != nil {
// 		t.Errorf("Unable to fetch: %s \n", e.Error())
// 	} else {
// 		toolkit.Println("======================")
// 		toolkit.Println(operation)
// 		toolkit.Println("======================")
// 		toolkit.Println(sintaks)
// 		toolkit.Println("Fetch OK. Result:")
// 		for _, val := range results {
// 			toolkit.Printf("%v \n",
// 				toolkit.JsonString(val))
// 		}
// 	}
// }
func TestSelectFilter(t *testing.T) {
	// t.Skip()
	skipIfConnectionIsNil(t)

	cursor, e := ctx.NewQuery().
		Select("_id", "nama", "amount").
		From(tableName).
		Where(dbox.And(dbox.Gt("amount", 150), dbox.Eq("nama", "buku"))).
		Cursor(nil)

	operation = "TestSelectFilter"
	sintaks = `
		ctx.NewQuery().
		Select("_id", "nama", "amount").
		From(tableName).
		Where(dbox.And(dbox.Gt("amount", 150000), 
			dbox.Eq("nama", "buku"))).
		Cursor(nil)`

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
		toolkit.Println(operation)
		toolkit.Println("======================")
		toolkit.Println(sintaks)
		toolkit.Println("Fetch OK. Result:")
		for _, val := range results {
			toolkit.Printf("%v \n",
				toolkit.JsonString(val))
		}
	}
}

// func TestSelectParameter(t *testing.T) {
// 	t.Skip()
// 	skipIfConnectionIsNil(t)

// 	cursor, e := ctx.NewQuery().
// 		Select("_id", "nama", "amount").
// 		From(tableName).
// 		Where(dbox.And(dbox.Gt("price", "@price"), dbox.Eq("status", "@status"))).
// 		Cursor(toolkit.M{}.Set("@price", 100000).Set("@status", "available"))

// 	operation = "TestSelectParameter"
// 	sintaks = `
// 		ctx.NewQuery().
// 		Select("_id", "nama", "amount").
// 		From(tableName).
// 		Where(dbox.And(dbox.Gt("price", "@price"),
// 			dbox.Eq("status", "@status"))).
// 		Cursor(toolkit.M{}.Set("@price", 100000).
// 			Set("@status", "available"))`

// 	if e != nil {
// 		t.Fatalf("Cursor error: " + e.Error())
// 	}
// 	defer cursor.Close()

// 	if cursor.Count() == 0 {
// 		t.Fatalf("No record found")
// 	}

// 	var results []toolkit.M
// 	e = cursor.Fetch(&results, 0, false)

// 	if e != nil {
// 		t.Errorf("Unable to fetch: %s \n", e.Error())
// 	} else {
// 		toolkit.Println("======================")
// 		toolkit.Println(operation)
// 		toolkit.Println("======================")
// 		toolkit.Println(sintaks)
// 		toolkit.Println("Fetch OK. Result:")
// 		for _, val := range results {
// 			toolkit.Printf("%v \n",
// 				toolkit.JsonString(val))
// 		}
// 	}
// }

// func TestFetch(t *testing.T) {
// 	t.Skip()
// 	skipIfConnectionIsNil(t)

// 	cursor, e := ctx.NewQuery().
// 		Select("_id", "nama", "quantity", "price", "amount").
// 		From(tableName).
// 		Cursor(nil)

// 	if e != nil {
// 		t.Fatalf("Cursor error: " + e.Error())
// 	}
// 	defer cursor.Close()

// 	if cursor.Count() == 0 {
// 		t.Fatalf("No record found")
// 	}

// 	var results []toolkit.M
// 	e = cursor.Fetch(&results, 2, false)

// 	if e != nil {
// 		t.Errorf("Unable to fetch: %s \n", e.Error())
// 	} else {
// 		toolkit.Println("======================")
// 		toolkit.Println("SELECT FETCH")
// 		toolkit.Println("======================")
// 		toolkit.Println("Fetch OK. Result:")
// 		for _, val := range results {
// 			toolkit.Printf("%v \n",
// 				toolkit.JsonString(val))
// 		}
// 	}
// }

// func TestInsert(t *testing.T) {
// 	t.Skip()
// 	var e error
// 	skipIfConnectionIsNil(t)

// 	es := []string{}
// 	qinsert := ctx.NewQuery().From(tableName).Insert()
// 	for i := 1; i <= 5; i++ {
// 		qty := toolkit.RandInt(10)
// 		price := toolkit.RandInt(10) * 50000
// 		amount := qty * price
// 		u := &Orders{
// 			toolkit.Sprintf("ord0%d", i+10),
// 			toolkit.Sprintf("item%d", i),
// 			qty,
// 			price,
// 			amount,
// 			toolkit.Sprintf("available"),
// 		}
// 		e = qinsert.Exec(toolkit.M{}.Set("data", u))
// 		if e != nil {
// 			es = append(es, toolkit.Sprintf("Insert fail %d: %s \n", i, e.Error()))
// 		}
// 	}

// 	if len(es) > 0 {
// 		t.Fatal(es)
// 	}
// 	TestSelect(t)
// }

// func TestUpdate(t *testing.T) {
// 	t.Skip()
// 	skipIfConnectionIsNil(t)
// 	e := ctx.NewQuery().
// 		Update().
// 		From(tableName).
// 		Where(dbox.Contains("nama", "item")).
// 		Exec(toolkit.M{}.Set("data", toolkit.M{}.Set("nama", "items")))

// 	if e != nil {
// 		t.Fatalf("Update fail: %s", e.Error())
// 	}
// 	TestSelect(t)
// }

// func TestDelete(t *testing.T) {
// 	t.Skip()
// 	skipIfConnectionIsNil(t)
// 	e := ctx.NewQuery().
// 		Delete().
// 		From(tableName).
// 		Where(dbox.Contains("nama", "item")).
// 		Exec(nil)
// 	if e != nil {
// 		t.Fatalf("Delete fail: %s", e.Error())
// 	}
// 	TestSelect(t)
// }

// func TestSave(t *testing.T) {
// 	t.Skip()
// 	skipIfConnectionIsNil(t)

// 	e := ctx.NewQuery().From(tableName).
// 		Save().
// 		Exec(toolkit.M{}.Set("data", toolkit.M{}.
// 		Set("_id", "ord010").
// 		Set("nama", "item").
// 		Set("quantity", 2).
// 		Set("price", 45000).
// 		Set("amount", 90000).
// 		Set("status", "out of stock")))
// 	if e != nil {
// 		t.Fatalf("Specific update fail: %s", e.Error())
// 	}
// 	TestSelect(t)

// 	e = ctx.NewQuery().From(tableName).
// 		Save().
// 		Exec(toolkit.M{}.Set("data", toolkit.M{}.
// 		Set("_id", "ord010").
// 		Set("nama", "item10").
// 		Set("quantity", 3).
// 		Set("price", 50000).
// 		Set("amount", 150000).
// 		Set("status", "available")))
// 	if e != nil {
// 		t.Fatalf("Specific update fail: %s", e.Error())
// 	}
// 	TestSelect(t)
// }

// func TestUpdateNoFilter(t *testing.T) {
// 	t.Skip()
// 	skipIfConnectionIsNil(t)
// 	data := Orders{}
// 	data.ID = "ord010"
// 	data.Nama = "item10"
// 	data.Quantity = 3
// 	data.Price = 75000
// 	data.Amount = 225000

// 	e := ctx.NewQuery().
// 		Update().
// 		From(tableName).
// 		Exec(toolkit.M{}.Set("data", data))

// 	if e != nil {
// 		t.Fatalf("Update fail: %s", e.Error())
// 	}
// 	TestSelect(t)
// }

// func TestDeleteNoFilter(t *testing.T) {
// 	t.Skip()
// 	skipIfConnectionIsNil(t)
// 	data := Orders{}
// 	data.ID = "ord010"

// 	e := ctx.NewQuery().
// 		Delete().
// 		From(tableName).
// 		Exec(toolkit.M{}.Set("data", data))
// 	if e != nil {
// 		t.Fatalf("Delete fail: %s", e.Error())
// 	}
// 	TestSelect(t)
// }

// func TestGetObj(t *testing.T) {
// 	t.Skip()
// 	toolkit.Printf("List Table : %v\n", ctx.ObjectNames(dbox.ObjTypeTable))

// 	toolkit.Printf("All Object : %v\n", ctx.ObjectNames(""))
// }

func TestClose(t *testing.T) {
	skipIfConnectionIsNil(t)
	ctx.Close()
}
