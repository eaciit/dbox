package odbctest

import (
	"github.com/eaciit/dbox"
	_ "github.com/eaciit/dbox/dbc/odbc"
	"github.com/eaciit/toolkit"
	"testing"
)

type Dummy struct {
	Id    int
	Name  string
	Email string
	Phone string
	Cvv   string
}

const (
	oracle    = "oci8"
	sqlServer = "mssql"
	postgre   = "postgre"
	mysql     = "mysql"
)

func prepareConnection() (dbox.IConnection, error) {
	settings := toolkit.M{"driver": oracle, "connector": "odbc", "dateformat": "2006-01-02 15:04:05"}
	ci := &dbox.ConnectionInfo{"oracle-dsn", "dboxtest", "dboxtest", "root", settings}
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
	// t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().
		From("customers").
		Select("datenow", "currency"). //tested and not working with 2 field
		// Order("unitsinstock"). //tested and not working
		// Skip(10). //tested and not working

		// Where(dbox.And(dbox.Gte("unitsinstock", 15), dbox.Eq("productname", "Chai"))).

		// Where(dbox.And(dbox.Gte("unitsinstock", "@stock"), dbox.Eq("productname", "@name"))).
		// Cursor(toolkit.M{}.Set("@stock", 15).Set("@name", "Chai"))
		// Take(10).
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

func TestInsert(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

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
}

func TestUpdate(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	// data := Dummy{}
	// data.Id = 101
	// data.Name = "Hendro Kartiko"
	// data.Email = "hendro.kartiko@persebaya.co.id"
	// data.Phone = "(08)218993837"
	// data.Cvv = "475"
	// e = c.NewQuery().From("dummy").Where(dbox.Eq("id", "101")).Update().Exec(toolkit.M{"data": data})
	// if e != nil {
	// 	t.Errorf("Unable to update: %s \n", e.Error())
	// }

	/*================ update WITHOUT where ===================================================*/

	data := Dummy{}
	data.Id = 101
	data.Name = "Aji Santoso"
	data.Email = "aji.santoso@persebaya.co.id"
	data.Phone = "(08)873739937"
	data.Cvv = "637"
	e = c.NewQuery().From("dummy").Update().Exec(toolkit.M{"data": data})
	if e != nil {
		t.Errorf("Unable to update: %s \n", e.Error())
	}
}

func TestSave(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	q := c.NewQuery().From("dummy").Save()
	dataSave := Dummy{}
	dataSave.Id = 101
	dataSave.Name = "Mursyid Effendi"
	dataSave.Email = "mursyid.effendi@persebaya.co.id"
	dataSave.Phone = "(08)98796957"
	dataSave.Cvv = "423"

	e = q.Exec(toolkit.M{"data": dataSave})
	if e != nil {
		t.Errorf("Unable to save data : %s \n", e.Error())
	}
}

func TestDelete(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	// e = c.NewQuery().From("dummy").Where(dbox.And(dbox.Eq("id", "101"), dbox.Eq("name", "Mursyid Effendi"))).Delete().Exec(nil)
	// if e != nil {
	// 	t.Errorf("Unable to delete data %s\n", e.Error())
	// 	return
	// }

	data := Dummy{}
	data.Id = 101

	e = c.NewQuery().From("dummy").Delete().Exec(toolkit.M{"data": data})
	if e != nil {
		t.Errorf("Unable to delete data %s\n", e.Error())
		return
	}
}
