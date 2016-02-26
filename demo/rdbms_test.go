package rdbms_demo

import (
	"fmt"
	"github.com/eaciit/dbox"
	_ "github.com/eaciit/dbox/dbc/mysql"
	"github.com/eaciit/toolkit"
	"testing"
	"time"
)

var ctx dbox.IConnection

const (
	config    bool   = true
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
}

func TestSelectFilter(t *testing.T) {
	// t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	csr, e := c.NewQuery().
		// Select("id", "name", "tanggal", "umur").
		From(tableName).
		// Where(dbox.And(dbox.Gt("amount", 100000), dbox.Eq("nama", "buku"))).
		// Where(dbox.Contains("nama", "tem", "pe")).
		// Order("nama").
		// Skip(2).
		// Take(5).
		Cursor(nil)
	// Where(dbox.And(dbox.Gt("price", "@price"), dbox.Eq("status", "@status"))).
	// Cursor(toolkit.M{}.Set("@price", 100000).Set("@status", "available"))
	// Where(dbox.And(dbox.Or(dbox.Eq("nama", "@name1"), dbox.Eq("nama", "@name2"),
	// dbox.Eq("nama", "@name3")), dbox.Lt("quantity", "@quantity"))).
	// Cursor(toolkit.M{}.Set("@name1", "buku").Set("@name2", "tas").
	// Set("@name3", "dompet").Set("@quantity", 4))

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
	// results := make([]User, 0)

	err := csr.Fetch(&results, 0, false)
	if err != nil {
		t.Errorf("Unable to fetch: %s \n", err.Error())
	} else {
		fmt.Println("======================")
		fmt.Println("Select with FILTER")
		fmt.Println("======================")

		fmt.Printf("Fetch N OK. Result:\n")
		for i := 0; i < len(results); i++ {
			fmt.Printf("%v \n", toolkit.JsonString(results[i]))
		}

	}
}

func TestInsert(t *testing.T) {
	t.Skip()
	var e error
	skipIfConnectionIsNil(t)

	es := []string{}
	qinsert := ctx.NewQuery().From(tableName).Insert()
	for i := 1; i <= 5; i++ {
		qty := toolkit.RandInt(10)
		price := toolkit.RandInt(10) * 50000
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
	TestSelect(t)
}

func TestUpdate(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)
	e := ctx.NewQuery().
		Update().
		From(tableName).
		SetConfig("multiexec", config).
		Where(dbox.Contains("nama", "item")).
		Exec(toolkit.M{}.Set("data", toolkit.M{}.Set("nama", "items")))

	if e != nil {
		t.Fatalf("Update fail: %s", e.Error())
	}
	TestSelect(t)
}

func TestDelete(t *testing.T) {
	t.Skip()
	skipIfConnectionIsNil(t)
	e := ctx.NewQuery().
		Delete().
		From(tableName).
		Where(dbox.Contains("nama", "item")).
		SetConfig("multiexec", config).
		Exec(nil)
	if e != nil {
		t.Fatalf("Delete fail: %s", e.Error())
	}
	TestSelect(t)
}

func TestSave(t *testing.T) {
	// t.Skip()
	skipIfConnectionIsNil(t)

	e := ctx.NewQuery().From(tableName).
		Save().
		Exec(toolkit.M{}.Set("data", toolkit.M{}.
		Set("_id", "ord010").
		Set("nama", "item").
		Set("quantity", 2).
		Set("price", 45000).
		Set("amount", 90000).
		Set("status", "out of stock")))
	if e != nil {
		t.Fatalf("Specific update fail: %s", e.Error())
	}
	TestSelect(t)

	e = ctx.NewQuery().From(tableName).
		Save().
		Exec(toolkit.M{}.Set("data", toolkit.M{}.
		Set("_id", "ord010").
		Set("nama", "item10").
		Set("quantity", 3).
		Set("price", 50000).
		Set("amount", 150000).
		Set("status", "available")))
	if e != nil {
		t.Fatalf("Specific update fail: %s", e.Error())
	}
	TestSelect(t)
}

func TestUpdateNoFilter(t *testing.T) {
	// t.Skip()
	skipIfConnectionIsNil(t)
	data := Orders{}
	data.ID = "ord010"
	data.Nama = "item10"
	data.Quantity = 3
	data.Price = 75000
	data.Amount = 225000

	e := ctx.NewQuery().
		Update().
		From(tableName).
		SetConfig("multiexec", config).
		Exec(toolkit.M{}.Set("data", data))

	if e != nil {
		t.Fatalf("Update fail: %s", e.Error())
	}
	TestSelect(t)
}

func TestDeleteNoFilter(t *testing.T) {
	// t.Skip()
	skipIfConnectionIsNil(t)
	data := Orders{}
	data.ID = "ord010"

	e := ctx.NewQuery().
		Delete().
		From(tableName).
		SetConfig("multiexec", config).
		Exec(toolkit.M{}.Set("data", data))
	if e != nil {
		t.Fatalf("Delete fail: %s", e.Error())
	}
	TestSelect(t)
}

func TestSelectAggregate(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Select("nama").
		Aggr(dbox.AggrSum, 1, "Total Item").
		Aggr(dbox.AggrMax, "amount", "Max Amount").
		Aggr(dbox.AggrSum, "amount", "Total Amount").
		Aggr(dbox.AggrAvr, "amount", "Average Amount").
		From("orders").
		Group("nama").
		Order("nama").
		Skip(2).
		Take(1).
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
	if err != nil {
		t.Errorf("Unable to fetch: %s \n", err.Error())
	} else {
		fmt.Println("======================")
		fmt.Println("QUERY AGGREGATION")
		fmt.Println("======================")
		for _, val := range results {
			fmt.Printf("Fetch N OK. Result: %v \n",
				toolkit.JsonString(val))
		}
	}
}

func TestProcedure(t *testing.T) {
	t.Skip()
	c, _ := prepareConnection()
	defer c.Close()

	//====================CALL MY SQL STORED PROCEDURE====================

	csr, e := c.NewQuery().
		Command("procedure", toolkit.M{}.
		Set("name", "updatedata").
		Set("orderparam", []string{"@idIn", "@idCondIn", "@nameIn", "@umurIn"}).
		Set("parms", toolkit.M{}.
		Set("@idIn", "ply030").
		Set("@idCondIn", "30").
		Set("@nameIn", "Payet").
		Set("@umurIn", 25))).
		Cursor(nil)

	if csr == nil {
		t.Errorf("Cursor not initialized", e.Error())
		return
	}
	defer csr.Close()

	results := make([]map[string]interface{}, 0)

	err := csr.Fetch(&results, 0, false)
	fmt.Println("Hasil Procedure : ", results)
	if err != nil {
		t.Errorf("Unable to fetch: %s \n", err.Error())
	} else {
		fmt.Println("======================")
		fmt.Println("STORED PROCEDURE")
		fmt.Println("======================")
		for _, val := range results {
			fmt.Printf("Fetch N OK. Result: %v \n",
				toolkit.JsonString(val))
		}
	}
}

func TestViewAllTables(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("unnable  to connect %s \n", e.Error())
	}
	defer c.Close()

	csr := c.ObjectNames(dbox.ObjTypeTable)

	for i := 0; i < len(csr); i++ {
		fmt.Printf("show name table %v \n", toolkit.JsonString(csr[i]))
	}

}

func TestViewProcedureName(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("unnable  to connect %s \n", e.Error())
	}
	defer c.Close()

	proc := c.ObjectNames(dbox.ObjTypeProcedure)

	for i := 0; i < len(proc); i++ {
		fmt.Printf("show name procdure %v \n", toolkit.JsonString(proc[i]))
	}

}

func TestViewName(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("unnable  to connect %s \n", e.Error())
	}
	defer c.Close()

	view := c.ObjectNames(dbox.ObjTypeView)

	for i := 0; i < len(view); i++ {
		fmt.Printf("show name view %v \n", toolkit.JsonString(view[i]))
	}

}

func TestAllObj(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("unnable  to connect %s \n", e.Error())
	}
	defer c.Close()

	all := c.ObjectNames(dbox.ObjTypeAll)

	fmt.Println(all)
	for i := 0; i < len(all); i++ {
		fmt.Printf("show objects %v \n", toolkit.JsonString(all[i]))
	}

}
