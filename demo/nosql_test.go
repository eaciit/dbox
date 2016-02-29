package nosql_demo

import (
	"fmt"
	"github.com/eaciit/dbox"
	_ "github.com/eaciit/dbox/dbc/mongo"
	"github.com/eaciit/toolkit"
	"testing"
)

func prepareConnection() (dbox.IConnection, error) {
	var config = toolkit.M{}.Set("timeout", 3)
	ci := &dbox.ConnectionInfo{"localhost:27017", "belajar", "", "", config}
	c, e := dbox.NewConnection("mongo", ci)
	if e != nil {
		return nil, e
	}

	e = c.Connect()
	if e != nil {
		return nil, e
	}

	return c, nil
}

func TestConnect(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Fatalf("Unable to connect: %s \n", e.Error())
		return
	}
	defer c.Close()
}

func TestSelect(t *testing.T) {
	// t.Skip()
	skipIfConnectionIsNil(t)

	cursor, e := ctx.NewQuery().
		Select("_id", "nama", "quantity", "price", "amount").
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
}

func TestFetch(t *testing.T) {
	// t.Skip()
	skipIfConnectionIsNil(t)

	cursor, e := ctx.NewQuery().
		Select("_id", "nama", "quantity", "price", "amount").
		From(tableName).
		Cursor(nil)

	if e != nil {
		t.Fatalf("Cursor error: " + e.Error())
	}
	defer cursor.Close()

	if cursor.Count() == 0 {
		t.Fatalf("No record found")
	}

	var results []toolkit.M
	e = cursor.Fetch(&results, 2, false)

	if e != nil {
		t.Errorf("Unable to fetch: %s \n", e.Error())
	} else {
		toolkit.Println("======================")
		toolkit.Println("SELECT FETCH")
		toolkit.Println("======================")
		toolkit.Println("Fetch OK. Result:")
		for _, val := range results {
			toolkit.Printf("%v \n",
				toolkit.JsonString(val))
		}
	}
}

/*func TestSelectAggregate(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	//fb := c.Fb()
	csr, e := c.NewQuery().
		Aggr(dbox.AggrSum, 1, "Sum").
		Aggr(dbox.AggrMax, "$fullname", "Name").
		From("ORMUsers").
		Group("enable").
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

	ds, e := csr.Fetch(nil, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch OK. Result: %v \n",
			toolkit.JsonString(ds.Data))

	}
}*/

/*func TestSelectAggregateUsingCommand(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	//fb := c.Fb()
	pipe := []toolkit.M{toolkit.M{}.Set("$group", toolkit.M{}.Set("_id", "$enable").Set("count", toolkit.M{}.Set("$sum", 1)))}
	csr, e := c.NewQuery().
		Command("pipe", pipe).
		From("ORMUsers").
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

	ds, e := csr.Fetch(nil, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch OK. Result: %v \n",
			toolkit.JsonString(ds.Data))
	}
}

func TestProcedure(t *testing.T) {
	c, _ := prepareConnection()
	defer c.Close()

	csr, e := c.NewQuery().Command("procedure", toolkit.M{}.Set("name", "spSomething").Set("parms", toolkit.M{}.Set("@name", "EACIIT"))).Cursor(nil)
	if e != nil {
		t.Error(e)
		return
	}
	defer csr.Close()

	ds, e := csr.Fetch(nil, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch OK. Result: %v \n",
			toolkit.JsonString(ds.Data))
	}

}*/

// func TestCRUD(t *testing.T) {
// 	//t.Skip()
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("Unable to connect %s \n", e.Error())
// 		return
// 	}
// 	defer c.Close()
// 	e = c.NewQuery().From("testtables").Delete().Exec(nil)
// 	if e != nil {
// 		t.Errorf("Unablet to clear table %s\n", e.Error())
// 		return
// 	}

// 	q := c.NewQuery().SetConfig("multiexec", true).From("testtables").Save()
// 	type user struct {
// 		Id    string `bson:"_id"`
// 		Title string
// 		Email string
// 	}
// 	for i := 1; i <= 10000; i++ {
// 		//go func(q dbox.IQuery, i int) {
// 		data := user{}
// 		data.Id = fmt.Sprintf("User-%d", i)
// 		data.Title = fmt.Sprintf("User-%d's name", i)
// 		data.Email = fmt.Sprintf("User-%d@myco.com", i)
// 		if i == 10 || i == 20 || i == 30 {
// 			data.Email = fmt.Sprintf("User-%d@myholding.com", i)
// 		}
// 		e = q.Exec(toolkit.M{
// 			"data": data,
// 		})
// 		if e != nil {
// 			t.Errorf("Unable to save: %s \n", e.Error())
// 		}
// 	}
// 	q.Close()

// 	data := user{}
// 	data.Id = fmt.Sprintf("User-15")
// 	data.Title = fmt.Sprintf("User Lima Belas")
// 	data.Email = fmt.Sprintf("user15@yahoo.com")
// 	e = c.NewQuery().From("testtables").Update().Exec(toolkit.M{"data": data})
// 	if e != nil {
// 		t.Errorf("Unable to update: %s \n", e.Error())
// 	}
// }

func TestGetObj(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()
	//ObjTypeTable, ObjTypeView, ObjTypeProcedure, ObjTypeAll
	toolkit.Printf("List Table : %v\n", c.ObjectNames(dbox.ObjTypeTable))
	toolkit.Printf("List Procedure : %v\n", c.ObjectNames(dbox.ObjTypeProcedure))
	toolkit.Printf("List All Object : %v\n", c.ObjectNames(""))
}
