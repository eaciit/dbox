package nosql_demo

import (
	"github.com/eaciit/dbox"
	_ "github.com/eaciit/dbox/dbc/mongo"
	"github.com/eaciit/toolkit"
	"testing"
)

var ctx dbox.IConnection
var sintaks string = `
		ctx.NewQuery().
		Select("_id", "nama", "amount").
		From(tableName).
		Cursor(nil)`
var operation string = "TestSelect"

const (
	tableName string = "orders"
	multi     bool   = true
)

func connect() error {
	var e error
	if ctx == nil {
		var config = toolkit.M{}.Set("timeout", 3)
		ctx, e = dbox.NewConnection("mongo",
			&dbox.ConnectionInfo{"localhost:27017", "belajar", "", "", config})
		if e != nil {
			return e
		}
	}
	e = ctx.Connect()
	return e
}

func skipIfConnectionIsNil(t *testing.T) {
	if ctx == nil {
		t.Skip()
	}
}

type Orders struct {
	Id       string `bson:"_id"`
	Nama     string `json:"nama", bson:"nama"`
	Quantity int    `json:"quantity", bson:"quantity"`
	Price    int    `json:"price", bson:"price"`
	Amount   int    `json:"amount", bson:"amount"`
	Status   string `json:"status", bson:"status"`
}

func TestConnect(t *testing.T) {
	e := connect()
	if e != nil {
		t.Errorf("Error connecting to database: %s \n", e.Error())
	} else {
		toolkit.Println("connected . . .")
	}
}

func TestSelect(t *testing.T) {
	// t.Skip()
	skipIfConnectionIsNil(t)

	cursor, e := ctx.NewQuery().
		Select("id", "nama", "amount").
		From(tableName).
		Where(dbox.And(dbox.Gt("amount", 150), dbox.Eq("nama", "buku"))).
		// Where(dbox.Contains("nama", "tem", "pe")).
		// Order("nama").
		// Skip(2).
		// Take(1).
		Cursor(nil)
	// Where(dbox.And(dbox.Gt("price", "@price"), dbox.Eq("status", "@status"))).
	// Cursor(toolkit.M{}.Set("@price", 100000).Set("@status", "available"))
	// Where(dbox.And(dbox.Or(dbox.Eq("nama", "@name1"), dbox.Eq("nama", "@name2"),
	// dbox.Eq("nama", "@name3")), dbox.Lt("quantity", "@quantity"))).
	// Cursor(toolkit.M{}.Set("@name1", "buku").Set("@name2", "tas").
	// Set("@name3", "dompet").Set("@quantity", 4))
	if e != nil {
		t.Fatalf("Cursor error: " + e.Error())
	}
	defer cursor.Close()

	var results []toolkit.M
	e = cursor.Fetch(&results, 0, false)

	operation = "Test Select Filter"
	sintaks = `
		ctx.NewQuery().
		Select("id", "nama", "amount").
		From(tableName).
		Where(dbox.And(dbox.Gt("amount", 150000), 
			dbox.Eq("nama", "buku"))).
		Cursor(nil)`

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

// func TestFetch(t *testing.T) {
// 	t.Skip()
// 	skipIfConnectionIsNil(t)

// 	cursor, e := ctx.NewQuery().
// 		Select("id", "nama", "quantity", "price", "amount").
// 		From(tableName).
// 		Cursor(nil)

// 	if e != nil {
// 		t.Fatalf("Cursor error: " + e.Error())
// 	}
// 	defer cursor.Close()

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
// 	qinsert := ctx.NewQuery().From(tableName).SetConfig("multiexec", multi).Insert()
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
// 		SetConfig("multiexec", multi).
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
// 		SetConfig("multiexec", multi).
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
// 		Set("id", "ord010").
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
// 		Set("id", "ord010").
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
// 	data.Id = "ord010"
// 	data.Nama = "item10"
// 	data.Quantity = 3
// 	data.Price = 75000
// 	data.Amount = 225000
// 	data.Status = "available"

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
// 	data.Id = "ord010"

// 	e := ctx.NewQuery().
// 		Delete().
// 		From(tableName).
// 		Exec(toolkit.M{}.Set("data", data))
// 	if e != nil {
// 		t.Fatalf("Delete fail: %s", e.Error())
// 	}
// 	TestSelect(t)
// }

// func TestSelectAggregate(t *testing.T) {
// 	t.Skip()
// 	skipIfConnectionIsNil(t)

// 	csr, e := ctx.NewQuery().
// 		Select("nama").
// 		// Aggr(dbox.AggrSum, 1, "Total Item").
// 		Aggr(dbox.AggrMax, "$amount", "Max Amount").
// 		Aggr(dbox.AggrSum, "$amount", "Total Amount").
// 		// Aggr(dbox.AggrAvr, "amount", "Average Amount").
// 		From(tableName).
// 		Group("nama").
// 		Order("nama").
// 		Cursor(nil)
// 	if e != nil {
// 		t.Errorf("Cursor pre error: %s \n", e.Error())
// 		return
// 	}
// 	if csr == nil {
// 		t.Errorf("Cursor not initialized")
// 		return
// 	}
// 	defer csr.Close()

// 	results := make([]map[string]interface{}, 0)

// 	err := csr.Fetch(&results, 0, false)
// 	if err != nil {
// 		t.Errorf("Unable to fetch: %s \n", err.Error())
// 	} else {
// 		toolkit.Println("======================")
// 		toolkit.Println("QUERY AGGREGATION")
// 		toolkit.Println("======================")
// 		toolkit.Println("Fetch N OK. Result:")
// 		for _, val := range results {
// 			toolkit.Printf("%v \n",
// 				toolkit.JsonString(val))
// 		}
// 	}
// }

// func TestProcedure(t *testing.T) {
// 	t.Skip()
// 	skipIfConnectionIsNil(t)

// 	//====================CALL MY SQL STORED PROCEDURE====================

// 	csr, e := ctx.NewQuery().
// 		Command("procedure", toolkit.M{}.
// 		Set("name", "updatedata").
// 		Set("orderparam", []string{"@idIn", "@idCondIn", "@nameIn", "@umurIn"}).
// 		Set("parms", toolkit.M{}.
// 		Set("@idIn", "ply030").
// 		Set("@idCondIn", "30").
// 		Set("@nameIn", "Payet").
// 		Set("@umurIn", 25))).
// 		Cursor(nil)

// 	if csr == nil {
// 		t.Errorf("Cursor not initialized", e.Error())
// 		return
// 	}
// 	defer csr.Close()

// 	results := make([]map[string]interface{}, 0)

// 	err := csr.Fetch(&results, 0, false)
// 	toolkit.Println("Hasil Procedure : ", results)
// 	if err != nil {
// 		t.Errorf("Unable to fetch: %s \n", err.Error())
// 	} else {
// 		toolkit.Println("======================")
// 		toolkit.Println("STORED PROCEDURE")
// 		toolkit.Println("======================")
// 		for _, val := range results {
// 			toolkit.Printf("Fetch N OK. Result: %v \n",
// 				toolkit.JsonString(val))
// 		}
// 	}
// }

// func TestViewAllTables(t *testing.T) {
// 	t.Skip()
// 	skipIfConnectionIsNil(t)

// 	csr := ctx.ObjectNames(dbox.ObjTypeTable)

// 	toolkit.Println("list of table : ")
// 	for i := 0; i < len(csr); i++ {
// 		toolkit.Printf("%v \n", toolkit.JsonString(csr[i]))
// 	}

// }

// func TestViewProcedureName(t *testing.T) {
// 	t.Skip()
// 	skipIfConnectionIsNil(t)

// 	proc := ctx.ObjectNames(dbox.ObjTypeProcedure)

// 	toolkit.Println("list of procedure : ")
// 	for i := 0; i < len(proc); i++ {
// 		toolkit.Printf("%v \n", toolkit.JsonString(proc[i]))
// 	}

// }

// func TestViewName(t *testing.T) {
// 	t.Skip()
// 	skipIfConnectionIsNil(t)

// 	view := ctx.ObjectNames(dbox.ObjTypeView)

// 	toolkit.Println("list of view : ")
// 	for i := 0; i < len(view); i++ {
// 		toolkit.Printf("%v \n", toolkit.JsonString(view[i]))
// 	}

// }

// func TestAllObj(t *testing.T) {
// 	t.Skip()
// 	skipIfConnectionIsNil(t)

// 	all := ctx.ObjectNames(dbox.ObjTypeAll)

// 	toolkit.Println("list of all objects : ")
// 	for i := 0; i < len(all); i++ {
// 		toolkit.Printf("%v \n", toolkit.JsonString(all[i]))
// 	}

// }

func TestClose(t *testing.T) {
	skipIfConnectionIsNil(t)
	ctx.Close()
}
