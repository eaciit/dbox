package mongo

import (
	"fmt"
	"github.com/eaciit/dbox"
	"github.com/eaciit/toolkit"
	"testing"
)

func prepareConnection() (dbox.IConnection, error) {
	var config = toolkit.M{}.Set("timeout", 3)
	ci := &dbox.ConnectionInfo{"localhost:27017", "eccolony", "", "", config}
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

func TestFilter(t *testing.T) {
	fb := dbox.NewFilterBuilder(new(FilterBuilder))
	fb.AddFilter(dbox.Or(
		dbox.Contains("_id", "1"),
		dbox.Contains("group", "adm", "test")))
	b, e := fb.Build()
	if e != nil {
		t.Errorf("Error %s", e.Error())
	} else {
		fmt.Printf("Result:\n%v\n", toolkit.JsonString(b))
	}
}

/*func TestSelect(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	csr, e := c.NewQuery().Select("_id", "title").From("appusers").Order("-title").
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
		t.Errorf("Unable to fetch all: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch all OK. Result: %d \n", len(ds.Data))
	}

	e = csr.ResetFetch()
	if e != nil {
		t.Errorf("Unable to reset fetch: %s \n", e.Error())
	}

	ds, e = csr.Fetch(nil, 3, false)
	if e != nil {
		t.Errorf("Unable to fetch N: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch N OK. Result: %v \n",
			ds.Data)
	}
}*/

func TestSelectFilter(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	csr, e := c.NewQuery().Select().
		Where(dbox.Contains("fullname", "43")).
		From("TestUsers").Cursor(nil)
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
	e = csr.Fetch(&results, 10, false)
	if e != nil {
		t.Errorf("Unable to fetch N1: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch N1 OK. Result: %v \n", results)
	}

	csr, e = c.NewQuery().
		Where(dbox.Contains("fullname", "43", "44")).
		From("TestUsers").Cursor(nil)
	if e != nil {
		t.Errorf("Cursor pre error: %s \n", e.Error())
		return
	}
	if csr == nil {
		t.Errorf("Cursor not initialized")
		return
	}
	defer csr.Close()

	results = make([]map[string]interface{}, 0)
	e = csr.Fetch(&results, 10, false)
	if e != nil {
		t.Errorf("Unable to fetch N1: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch N2 OK. Result: %v \n", results)
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
