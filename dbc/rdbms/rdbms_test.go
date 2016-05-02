package rdbms

import ( 
	"github.com/eaciit/dbox"
	// "github.com/eaciit/toolkit"
	"testing"
	"fmt"
)

func prepareConnection() (dbox.IConnection, error) {
	ci := &dbox.ConnectionInfo{"localhost:8088", "eccolony", "root", "", nil}
	c, e := dbox.NewConnection("mysql", ci) 
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
	//fmt.Println(c)
	if e != nil {
		t.Errorf("Unable to connect: %s \n", e.Error())
		fmt.Println(e)
	}else{
		fmt.Println(c)
	}
	defer c.Close()
}

// func TestFilter(t *testing.T) {
// 	fb := dbox.NewFilterBuilder(new(FilterBuilder))
// 	fb.AddFilter(dbox.Or(
// 		dbox.Eq("_id", 1),
// 		dbox.Eq("group", "administrators")))
// 	b, e := fb.Build()
// 	if e != nil {
// 		t.Errorf("Error %s", e.Error())
// 	} else {
// 		fmt.Printf("Result:\n%v\n", toolkit.JsonString(b))
// 	}
// }

func TestSelect(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().Select("_id", "email").From("testtables").Cursor(nil)
	if e != nil {
		t.Errorf("Cursor pre error: %s \n", e.Error())
		return
	}
	if csr == nil {
		t.Errorf("Cursor not initialized")
		return
	}
	defer csr.Close()

	//rets := []toolkit.M{}

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
}

// func TestSelectFilter(t *testing.T) {
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("Unable to connect %s \n", e.Error())
// 		return
// 	}
// 	defer c.Close()

// 	csr, e := c.NewQuery().
// 		//Select("_id", "email").
// 		Where(dbox.Eq("email", "arief@eaciit.com")).
// 		From("appusers").Cursor(nil)
// 	if e != nil {
// 		t.Errorf("Cursor pre error: %s \n", e.Error())
// 		return
// 	}
// 	if csr == nil {
// 		t.Errorf("Cursor not initialized")
// 		return
// 	}
// 	defer csr.Close()

// 	//rets := []toolkit.M{}

// 	ds, e := csr.Fetch(nil, 0, false)
// 	if e != nil {
// 		t.Errorf("Unable to fetch: %s \n", e.Error())
// 	} else {
// 		fmt.Printf("Fetch OK. Result: %v \n",
// 			toolkit.JsonString(ds.Data[0]))

// 	}
// }

/*
func TestSelectAggregate(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	fb := c.Fb()
	csr, e := c.NewQuery().
		//Select("_id", "email").
		//Where(c.Fb().Eq("email", "arief@eaciit.com")).
		Aggr(dbox.AggSum, 1, "Count").
		Aggr(dbox.AggSum, 1, "Avg").
		From("appusers").
		Group("").
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

	//rets := []toolkit.M{}

	ds, e := csr.Fetch(nil, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch OK. Result: %v \n",
			toolkit.JsonString(ds.Data[0]))

	}
}
*/

func TestCRUD(t *testing.T) {
	//t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()
	e = c.NewQuery().From("testtables").Delete().Exec(nil)
	if e != nil {
		t.Errorf("Unablet to clear table %s\n", e.Error())
		return
	}

	// q := c.NewQuery().SetConfig("multiexec", true).From("testtables").Save()
	// type user struct {
	// 	Id    string `bson:"_id"`
	// 	Title string
	// 	Email string
	// }
	// for i := 1; i <= 10000; i++ {
	// 	//go func(q dbox.IQuery, i int) {
	// 	data := user{}
	// 	data.Id = fmt.Sprintf("User-%d", i)
	// 	data.Title = fmt.Sprintf("User-%d's name", i)
	// 	data.Email = fmt.Sprintf("User-%d@myco.com", i)
	// 	if i == 10 || i == 20 || i == 30 {
	// 		data.Email = fmt.Sprintf("User-%d@myholding.com", i)
	// 	}
	// 	e = q.Exec(toolkit.M{
	// 		"data": data,
	// 	})
	// 	if e != nil {
	// 		t.Errorf("Unable to save: %s \n", e.Error())
	// 	}
	// }
	// q.Close()

	// data := user{}
	// data.Id = fmt.Sprintf("User-15")
	// data.Title = fmt.Sprintf("User Lima Belas")
	// data.Email = fmt.Sprintf("user15@yahoo.com")
	// e = c.NewQuery().From("testtables").Update().Exec(toolkit.M{"data": data})
	// if e != nil {
	// 	t.Errorf("Unable to update: %s \n", e.Error())
	// }
}
