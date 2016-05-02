package rdbms_demo

import (
	"github.com/eaciit/dbox"
	_ "github.com/eaciit/dbox/dbc/mysql"
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
	tableName string = "Orders"
)

func connect() error {
	var e error
	if ctx == nil {
		ctx, e = dbox.NewConnection("mysql",
			&dbox.ConnectionInfo{"localhost:3306", "test", "root", "", nil})
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
	ID       string
	Nama     string
	Quantity int
	Price    int
	Amount   int
	Status   string
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
		Cursor(nil)
	if e != nil {
		t.Fatalf("Cursor error: " + e.Error())
	}
	defer cursor.Close()

	var results []toolkit.M
	e = cursor.Fetch(&results, 0, false)

	if e != nil {
		t.Errorf("Unable to fetch: %s \n", e.Error())
	} else {
		toolkit.Println("======================")
		toolkit.Println(operation)
		toolkit.Println("======================")
		toolkit.Println(sintaks)
		toolkit.Println("Number of rows", cursor.Count())
		toolkit.Println("Fetch OK. Result:")
		for _, val := range results {
			toolkit.Printf("%v \n",
				toolkit.JsonString(val))
		}
	}
}

func TestFetch(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)

	cursor, e := ctx.NewQuery().
		Select("id", "nama", "quantity", "price", "amount").
		From(tableName).
		Cursor(nil)

	if e != nil {
		t.Fatalf("Cursor error: " + e.Error())
	}
	defer cursor.Close()

	var results []toolkit.M
	e = cursor.Fetch(&results, 2, false)

	operation = "Test Fetch"
	sintaks = `
		Select("id", "nama", "quantity", "price", "amount").
		From(tableName).
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

func TestSelectFilter(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)

	cursor, e := ctx.NewQuery().
		Select("id", "nama", "amount").
		From(tableName).
		Where(dbox.And(dbox.Gt("amount", 150), dbox.Eq("nama", "buku"))).
		Cursor(nil)
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
		Where(dbox.And(dbox.Gt("amount", 150),
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

func TestSelectParameter(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)

	cursor, e := ctx.NewQuery().
		Select("id", "nama", "amount").
		From(tableName).
		Where(dbox.And(dbox.Gt("price", "@price"), dbox.Eq("status", "@status"))).
		Cursor(toolkit.M{}.Set("@price", 100).Set("@status", "available"))
	if e != nil {
		t.Fatalf("Cursor error: " + e.Error())
	}
	defer cursor.Close()

	var results []toolkit.M
	e = cursor.Fetch(&results, 0, false)
	operation = "Test Select Parameter"
	sintaks = `
		ctx.NewQuery().
		Select("id", "nama", "amount").
		From(tableName).
		Where(dbox.And(dbox.Gt("price", "@price"),
			dbox.Eq("status", "@status"))).
		Cursor(toolkit.M{}.Set("@price", 100).
			Set("@status", "available"))`

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

func TestLimitDataSelections(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)

	cursor, e := ctx.NewQuery().
		Select("id", "nama", "amount").
		From(tableName).
		Order("nama").
		Skip(1).
		Take(3).
		Cursor(nil)
	if e != nil {
		t.Fatalf("Cursor error: " + e.Error())
	}
	defer cursor.Close()

	var results []toolkit.M
	e = cursor.Fetch(&results, 0, false)
	operation = "Test Limit Data Selections"
	sintaks = `
		ctx.NewQuery().
		Select("id", "nama", "amount").
		From(tableName).
		Skip(1).
		Take(3).
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

func TestFreeQuery(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)

	csr, e := ctx.NewQuery().
		Command("freequery", toolkit.M{}.
		Set("syntax", "select nama, amount from orders where nama = 'buku'")).
		Cursor(nil)

	if csr == nil {
		t.Errorf("Cursor not initialized", e.Error())
		return
	}
	defer csr.Close()

	results := make([]map[string]interface{}, 0)

	err := csr.Fetch(&results, 0, false)

	sintaks = `
		ctx.NewQuery().
		Command("freequery", toolkit.M{}.
		Set("syntax", "select nama, amount
			from orders where nama = 'buku'")).
		Cursor(nil)`
	if err != nil {
		t.Errorf("Unable to fetch: %s \n", err.Error())
	} else {
		toolkit.Println("======================")
		toolkit.Println("TEST FREE QUERY")
		toolkit.Println("======================")
		toolkit.Println(sintaks)
		toolkit.Println("Fetch N OK. Result: ")
		for _, val := range results {
			toolkit.Printf("%v \n",
				toolkit.JsonString(val))
		}
	}
}

func TestSelectAggregate(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)

	csr, e := ctx.NewQuery().
		Select("nama").
		// Aggr(dbox.AggrSum, 1, "Total Item").
		Aggr(dbox.AggrMax, "amount", "Max Amount").
		Aggr(dbox.AggrSum, "amount", "Total Amount").
		// Aggr(dbox.AggrAvr, "amount", "Average Amount").
		From(tableName).
		Group("nama").
		Order("-nama").
		Cursor(nil)
	if e != nil {
		t.Errorf("Cursor pre error: %s \n", e.Error())
		return
	}
	if csr == nil {
		t.Errorf("Cursor not initialized")
		return
	}
	defer csr.Close()

	results := make([]map[string]interface{}, 0)

	err := csr.Fetch(&results, 0, false)
	sintaks = `
		ctx.NewQuery().
		Select("nama").
		Aggr(dbox.AggrMax, "amount", "Max Amount").
		Aggr(dbox.AggrSum, "amount", "Total Amount").
		From(tableName).
		Group("nama").
		Order("-nama").
		Cursor(nil)`
	if err != nil {
		t.Errorf("Unable to fetch: %s \n", err.Error())
	} else {
		toolkit.Println("======================")
		toolkit.Println("QUERY AGGREGATION")
		toolkit.Println("======================")
		toolkit.Println("Fetch N OK. Result:")
		for _, val := range results {
			toolkit.Printf("%v \n",
				toolkit.JsonString(val))
		}
	}
}

func TestViewAllTables(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)

	csr := ctx.ObjectNames(dbox.ObjTypeTable)

	toolkit.Println("list of table : ")
	for i := 0; i < len(csr); i++ {
		toolkit.Printf("%v \n", toolkit.JsonString(csr[i]))
	}
}

func TestViewProcedureName(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)

	proc := ctx.ObjectNames(dbox.ObjTypeProcedure)

	toolkit.Println("list of procedure : ")
	for i := 0; i < len(proc); i++ {
		toolkit.Printf("%v \n", toolkit.JsonString(proc[i]))
	}

}

func TestViewName(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)

	view := ctx.ObjectNames(dbox.ObjTypeView)

	toolkit.Println("list of view : ")
	for i := 0; i < len(view); i++ {
		toolkit.Printf("%v \n", toolkit.JsonString(view[i]))
	}

}

func TestAllObj(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)

	all := ctx.ObjectNames(dbox.ObjTypeAll)

	toolkit.Println("list of all objects : ")
	for i := 0; i < len(all); i++ {
		toolkit.Printf("%v \n", toolkit.JsonString(all[i]))
	}

}

func TestProcedure(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)

	csr, e := ctx.NewQuery().
		Command("procedure", toolkit.M{}.
		Set("name", "updatedatademo").
		Set("orderparam", []string{"@idCondIn", "@amountIn", "@namaIn"}).
		Set("parms", toolkit.M{}.
		Set("@idCondIn", "ord001").
		Set("@amountIn", 300).
		Set("@namaIn", "buku anak anak"))).
		Cursor(nil)

	if csr == nil {
		t.Errorf("Cursor not initialized", e.Error())
		return
	}
	defer csr.Close()

	results := make([]map[string]interface{}, 0)

	err := csr.Fetch(&results, 0, false)
	operation = "Test Stored Procedure"
	sintaks = `
		ctx.NewQuery().
		Command("procedure", toolkit.M{}.
		Set("name", "updatedatademo").
		Set("orderparam", []string{"@idCondIn", "@amountIn", "@namaIn"}).
		Set("parms", toolkit.M{}.
		Set("@idCondIn", "ord001").
		Set("@amountIn", 300).
		Set("@namaIn", "buku anak anak"))).
		Cursor(nil)`
	if err != nil {
		t.Errorf("Unable to fetch: %s \n", err.Error())
	} else {
		TestSelect(t)
	}
}

func TestInsert(t *testing.T) {
	t.Skip()
	var e error
	skipIfConnectionIsNil(t)

	es := []string{}
	qinsert := ctx.NewQuery().From(tableName).Insert()
	for i := 1; i <= 3; i++ {
		qty := toolkit.RandInt(10)
		price := toolkit.RandInt(10) * 50
		amount := qty * price
		u := &Orders{
			toolkit.Sprintf("ord0%d", i+10),
			toolkit.Sprintf("item%d", i),
			qty,
			price,
			amount,
			toolkit.Sprintf("available"),
		}
		e = qinsert.Exec(toolkit.M{}.Set("data", u))
		if e != nil {
			es = append(es, toolkit.Sprintf("Insert fail %d: %s \n", i, e.Error()))
		}
	}

	if len(es) > 0 {
		t.Fatal(es)
	}

	operation = "Test Insert"
	sintaks = `
		ctx.NewQuery().From(tableName).Insert().
		Exec(toolkit.M{}.Set("data", u))`
	TestSelect(t)
}

func TestUpdate(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)
	e := ctx.NewQuery().
		Update().
		From(tableName).
		Where(dbox.Contains("nama", "item")).
		Exec(toolkit.M{}.Set("data", toolkit.M{}.Set("nama", "itemUpdate")))

	if e != nil {
		t.Fatalf("Update fail: %s", e.Error())
	}
	operation = "Test Update"
	sintaks = `
		ctx.NewQuery().
		Update().
		From(tableName).
		Where(dbox.Contains("nama", "item")).
		Exec(toolkit.M{}.Set("data", toolkit.M{}.
			Set("nama", "itemUpdate")))`
	TestSelect(t)
}

func TestDelete(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)
	e := ctx.NewQuery().
		Delete().
		From(tableName).
		Where(dbox.Contains("nama", "item")).
		Exec(nil)
	if e != nil {
		t.Fatalf("Delete fail: %s", e.Error())
	}
	operation = "Test Delete"
	sintaks = `
		ctx.NewQuery().
		Delete().
		From(tableName).
		Where(dbox.Contains("nama", "item")).
		Exec(nil)`
	TestSelect(t)
}

func TestSave(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)

	e := ctx.NewQuery().From(tableName).
		Save().
		Exec(toolkit.M{}.Set("data", toolkit.M{}.
		Set("id", "ord010").
		Set("nama", "itemInsertSave").
		Set("quantity", 2).
		Set("price", 45).
		Set("amount", 90).
		Set("status", "out of stock")))
	if e != nil {
		t.Fatalf("Specific update fail: %s", e.Error())
	}
	operation = "Test SAVE Insert"
	sintaks = `
		ctx.NewQuery().From(tableName).
		Save().
		Exec(toolkit.M{}.Set("data", toolkit.M{}.
		Set("id", "ord010").
		Set("nama", "itemInsertSave").
		Set("quantity", 2).
		Set("price", 45).
		Set("amount", 90).
		Set("status", "out of stock")))`
	TestSelect(t)

	data := Orders{}
	data.ID = "ord010"
	data.Nama = "itemUpdateSave"
	data.Quantity = 3
	data.Price = 75
	data.Amount = 225
	data.Status = "available"

	e = ctx.NewQuery().From(tableName).
		Save().
		Exec(toolkit.M{}.Set("data", data))
	if e != nil {
		t.Fatalf("Specific update fail: %s", e.Error())
	}
	operation = "Test SAVE Update"
	sintaks = `
		ctx.NewQuery().From(tableName).
		Save().
		Exec(toolkit.M{}.Set("data", data))`
	TestSelect(t)
}

func TestUpdateNoFilter(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)
	data := Orders{}
	data.ID = "ord010"
	data.Nama = "itemUpdateNoFilter"
	data.Quantity = 3
	data.Price = 75
	data.Amount = 225
	data.Status = "available"

	e := ctx.NewQuery().
		Update().
		From(tableName).
		Exec(toolkit.M{}.Set("data", data))

	if e != nil {
		t.Fatalf("Update fail: %s", e.Error())
	}
	operation = "Test UPDATE No Filter"
	sintaks = `
		ctx.NewQuery().
		Update().
		From(tableName).
		Exec(toolkit.M{}.Set("data", data))`
	TestSelect(t)
}

func TestDeleteNoFilter(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)
	data := Orders{}
	data.ID = "ord010"

	e := ctx.NewQuery().
		Delete().
		From(tableName).
		Exec(toolkit.M{}.Set("data", data))
	if e != nil {
		t.Fatalf("Delete fail: %s", e.Error())
	}
	operation = "Test DELETE NoFilter"
	sintaks = `
		ctx.NewQuery().
		Update().
		From(tableName).
		Exec(toolkit.M{}.Set("data", data))`
	TestSelect(t)
}

func TestClose(t *testing.T) {
	skipIfConnectionIsNil(t)
	ctx.Close()
}
