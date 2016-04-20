package odbctest

import (
	"github.com/eaciit/dbox"
	_ "github.com/eaciit/dbox/dbc/odbc"
	"github.com/eaciit/toolkit"
	"testing"
)

func prepareConnection() (dbox.IConnection, error) {
	settings := toolkit.M{"driver": "mysql", "connector": "odbc"}
	ci := &dbox.ConnectionInfo{"mysql-dsn", "test", "root", "", settings}
	c, e := dbox.NewConnection("odbc", ci)
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
		toolkit.Println(e)
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
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().
		From("dummy").
		Select("id", "name", "cvv"). //tested and not working with 2 field
		// Order("unitsinstock"). //tested and not working
		Skip(10). //tested and not working

		// Where(dbox.And(dbox.Gte("unitsinstock", 15), dbox.Eq("productname", "Chai"))).

		// Where(dbox.And(dbox.Gte("unitsinstock", "@stock"), dbox.Eq("productname", "@name"))).
		// Cursor(toolkit.M{}.Set("@stock", 15).Set("@name", "Chai"))
		Take(10).
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

	result := make([]map[string]interface{}, 0) //[]toolkit.M{}
	e = csr.Fetch(&result, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch all: %s \n", e.Error())
	} else {
		toolkit.Printf("Fetch all OK. Result: %v \n", toolkit.JsonString(result))
	}

	e = csr.ResetFetch()
	if e != nil {
		t.Errorf("Unable to reset fetch: %s \n", e.Error())
	}

	/*ds, e = csr.Fetch(nil, 3, false)
	if e != nil {
		t.Errorf("Unable to fetch N: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch N OK. Result: %v \n",
			ds.Data)
	}*/
}

func TestFreeQuery(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Command("freequery", toolkit.M{}.
		Set("syntax", "select ProductID, ProductName from products where productname = 'Genen Shouyu'")).
		Cursor(nil)

	if csr == nil {
		t.Errorf("Cursor not initialized", e.Error())
		return
	}
	defer csr.Close()

	results := make([]map[string]interface{}, 0)
	err := csr.Fetch(&results, 0, false)
	if err != nil {
		t.Errorf("Unable to fetch: %s \n", err.Error())
	} else {
		toolkit.Println("======================")
		toolkit.Println("TEST FREE QUERY")
		toolkit.Println("======================")
		toolkit.Println("Fetch N OK. Result: ")
		for _, val := range results {
			toolkit.Printf("%v \n",
				toolkit.JsonString(val))
		}
	}
}

func TestSelectAggregate(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Select("nama"). //not working if used field
		Aggr(dbox.AggrMax, "amount", "MaxAmount").
		Aggr(dbox.AggrSum, "amount", "TotalAmount").
		From("orders").
		Group("nama").
		// Order("-nama").
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

func TestCRUD(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()
	type Dummy struct {
		Id    int
		Name  string
		Email string
		Phone string
		Cvv   string
	}

	/*===============================INSERT==============================*/
	q := c.NewQuery().From("dummy").Insert()
	dataInsert := Dummy{}
	dataInsert.Id = 101
	dataInsert.Name = "Bejo Sugiantoro"
	dataInsert.Email = "bejo.sugiantoro@persebaya.co.id"
	dataInsert.Phone = "(08)56497989"
	dataInsert.Cvv = "856"

	e = q.Exec(toolkit.M{"data": dataInsert})
	if e != nil {
		t.Errorf("Unable to insert data : %s \n", e.Error())
	}

	/* ===============================SAVE DATA============================== */
	// q := c.NewQuery().SetConfig("multiexec", false).From("coba").Save()
	// dataInsert := Coba{}
	// dataInsert.Id = fmt.Sprintf("3")
	// dataInsert.Name = fmt.Sprintf("update data")

	// q := c.NewQuery().SetConfig("multiexec", false).From("NoID").Save()
	// dataInsert := NoID{}
	// dataInsert.Aidi = fmt.Sprintf("40")
	// dataInsert.Name = fmt.Sprintf("no update")

	// e = q.Exec(toolkit.M{"data": dataInsert})
	// if e != nil {
	// 	t.Errorf("Unable to insert data : %s \n", e.Error())
	// }

	/* ===============================UPDATE============================== */

	// data := User{}
	// data.Id = "40"
	// data.Name = "player40"
	// data.Tanggal = time.Now()
	// data.Umur = 24
	// e = c.NewQuery().From("tes").Where(dbox.Eq("id", "30")).Update().Exec(toolkit.M{"data": data})
	// if e != nil {
	// 	t.Errorf("Unable to update: %s \n", e.Error())
	// }
	// with where and data

	// data := Coba{}
	// data.Id = "1"
	// data.Name = "Jamme"
	// e = c.NewQuery().From("coba").Where(dbox.Eq("id", "1")).Update().Exec(toolkit.M{"data": data})
	// if e != nil {
	// 	t.Errorf("Unable to update: %s \n", e.Error())
	// }

	// ===============================UPDATE ALL ID==============================
	// data := UpdateID{}
	// fmt.Println(data)
	// for i := 1; i < 23; i++ {
	// 	data := UpdateID{}
	// 	if i < 10 {
	// 		data.Id = "ply00" + strconv.Itoa(i)
	// 	} else {
	// 		data.Id = "ply0" + strconv.Itoa(i)
	// 	}

	// 	e = c.NewQuery().From("tes").Where(dbox.Eq("id", i)).Update().Exec(toolkit.M{"data": data})
	// 	if e != nil {
	// 		t.Errorf("Unable to update: %s \n", e.Error())
	// 	}

	// }

	// // ===============================DELETE==============================
	// e = c.NewQuery().From("tes").Where(dbox.And(dbox.Eq("id", "2"), dbox.Eq("name", "Thuram"))).Delete().Exec(nil)
	// if e != nil {
	// 	t.Errorf("Unable to delete table %s\n", e.Error())
	// 	return
	// }

	// data := User{}
	// data.Id = "40"

	// e = c.NewQuery().From("tes").Delete().Exec(toolkit.M{"data": data})
	// if e != nil {
	// 	t.Errorf("Unable to delete table %s\n", e.Error())
	// 	return
	// }
}
