package mssql

import (
	"fmt"
	"github.com/eaciit/dbox"
	"github.com/eaciit/toolkit"
	"testing"
	// "time"
)

func prepareConnection() (dbox.IConnection, error) {
	//ci := &dbox.ConnectionInfo{"localhost", "Tes", "sa", "Password.Sql", nil}
	ci := &dbox.ConnectionInfo{"localhost", "test", "sa", "budi123", nil}
	c, e := dbox.NewConnection("mssql", ci)
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
		t.Errorf("Unable to connect: %s \n", e.Error())
		fmt.Println(e)
	} else {
		// fmt.Println(c)
	}
	defer c.Close()
}

// func TestSelect(t *testing.T) {
// 	c, e := prepareConnection()

// 	if e != nil {
// 		t.Errorf("Unable to connect %s \n", e.Error())
// 	}
// 	defer c.Close()

// 	// csr, e := c.NewQuery().Select().From("tes").Where(dbox.Eq("id", "3")).Cursor(nil)
// 	csr, e := c.NewQuery().Select("id", "name", "tanggal").From("tes").Cursor(nil)

// 	if e != nil {
// 		t.Errorf("Cursor pre error: %s \n", e.Error())
// 		return
// 	}
// 	if csr == nil {
// 		t.Errorf("Cursor not initialized")
// 		return
// 	}
// 	defer csr.Close()

// 	// //rets := []toolkit.M{}

// 	results := make([]map[string]interface{}, 0)
// 	err := csr.Fetch(&results, 0, false)
// 	if err != nil {
// 		t.Errorf("Unable to fetch: %s \n", err.Error())
// 	} else {
// 		fmt.Println("======================")
// 		fmt.Println("Select with FILTER")
// 		fmt.Println("======================")
// 		for _, val := range results {
// 			fmt.Printf("Fetch N OK. Result: %v \n",
// 				toolkit.JsonString(val))
// 		}
// 	}
// }

// func TestSelectFilter(t *testing.T) {
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("Unable to connect %s \n", e.Error())
// 		return
// 	}
// 	defer c.Close()

// 	csr, e := c.NewQuery().
// 		Select("id", "name").
// 		Where(dbox.And(dbox.Eq("id", "1"),(dbox.Eq("name","a")))).
// 		From("tes").Cursor(nil)
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
// 		fmt.Printf("Fetch N OK. Result: %v \n",
// 			ds.Data)
// 	}
// }

// func TestSelectAggregate(t *testing.T) {
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("Unable to connect %s \n", e.Error())
// 	}
// 	defer c.Close()

// 	fb := c.Fb()
// 	csr, e := c.NewQuery().
// 		//Select("_id", "email").
// 		//Where(c.Fb().Eq("email", "arief@eaciit.com")).
// 		Aggr(dbox.AggSum, 1, "Count").
// 		Aggr(dbox.AggSum, 1, "Avg").
// 		From("appusers").
// 		Group("").
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

// 	//rets := []toolkit.M{}

// 	ds, e := csr.Fetch(nil, 0, false)
// 	if e != nil {
// 		t.Errorf("Unable to fetch: %s \n", e.Error())
// 	} else {
// 		fmt.Printf("Fetch OK. Result: %v \n",
// 			toolkit.JsonString(ds.Data[0]))

// 	}
// }

// func TestCRUD(t *testing.T) {
// 	//t.Skip()
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("Unable to connect %s \n", e.Error())
// 		return
// 	}
// 	defer c.Close()

// 	e = c.NewQuery().From("tes").Where(dbox.And(dbox.Eq("id", "1133331"),dbox.Eq("name", "testing"))).Delete().Exec(nil)
// 	if e != nil {
// 		t.Errorf("Unablet to delete table %s\n", e.Error())
// 		return
// 	}
// 	defer c.Close()

// 	e = c.NewQuery().From("tes").Delete().Exec(nil)
// 	if e != nil {
// 		t.Errorf("Unablet to clear table %s\n", e.Error())
// 		return
// 	}

// 	defer c.Close()

// 	// q := c.NewQuery().SetConfig("multiexec", true).From("tes").Save()
// 	type user struct {
// 		// Id     int
// 		Name   string
// 		// Date   time.Time
// 	}

// 	// // 	//go func(q dbox.IQuery, i int) {
// 		data := user{}
// 		// data.Id =111
// 		data.Name = "testingupdate2222"
// 		// data.Date = time.Now()

//   		// e = q.Exec(toolkit.M{
// 		// 	"data": data,
// 		// })
// 		// if e != nil {
// 		// 	t.Errorf("Unable to save: %s \n", e.Error())
// 		// }
// 		 e = c.NewQuery().From("tes").Where(dbox.Eq("id", "111")).Update().Exec(toolkit.M{"data": data})
// 		if e != nil {
// 			t.Errorf("Unable to update: %s \n", e.Error())
// 		}

// }

func TestStartorEndWith(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("unnable  to connect %s \n", e.Error())
	}
	defer c.Close()

	//csr, e := c.NewQuery().Select("id", "name", "umur").From("tes").Where(dbox.Startwith("name", "Co")).Cursor(nil)
	csr, e := c.NewQuery().Select("id", "name", "umur").From("tes").Where(dbox.Endwith("name", "ey")).Cursor(nil)

	if e != nil {
		t.Errorf("cursor pre error : %s \n", e.Error())
		return
	}

	if csr == nil {
		t.Errorf("cursor not initialized")
	}

	results := make([]map[string]interface{}, 0)

	err := csr.Fetch(&results, 0, false)
	if err != nil {
		t.Errorf("unnable to fetch: %s \n", err.Error())
	} else {
		fmt.Println("===========================")
		fmt.Println("contain data")
		fmt.Println("===========================")

		fmt.Println("fetch N Ok. Result :\n")

		for i := 0; i < len(results); i++ {
			fmt.Printf("%v \n", toolkit.JsonString(results[i]))
		}
	}

}
