package csv

import (
	// "encoding/json"
	"fmt"
	"github.com/eaciit/dbox"
	"github.com/eaciit/toolkit"
	"testing"
	"time"
)

func prepareConnection() (dbox.IConnection, error) {
	var config = map[string]interface{}{"useheader": true, "delimiter": ",", "dateformat": "MM-dd-YYYY", "newfile": true}
	ci := &dbox.ConnectionInfo{"E:\\data\\sample\\TEST1234.csv", "", "", "", config}
	c, e := dbox.NewConnection("csv", ci)
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
	t.Skip("Just Skip Test")
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect: %s \n", e.Error())
	}
	c.Close()
	time.Sleep(100 * time.Millisecond)
}

func TestFilter(t *testing.T) {
	t.Skip("Just Skip Test")
	fb := dbox.NewFilterBuilder(new(FilterBuilder))
	fb.AddFilter(dbox.Or(
		dbox.Contains("regfield", "1"),
		dbox.Ne("nefield", 1),
		dbox.Eq("group", "administrators")))
	b, e := fb.Build()
	if e != nil {
		t.Errorf("Error %s", e.Error())
	} else {
		fmt.Printf("Result:\n%v\n", toolkit.JsonString(b))
	}

	fb = dbox.NewFilterBuilder(new(FilterBuilder))
	fb.AddFilter(dbox.And(dbox.Or(dbox.Eq("EmployeeId", "101-102-10"), dbox.Eq("EmployeeId", "101-102-3"), dbox.Eq("EmployeeId", "101-102-4")), dbox.Eq("Age", "30")))
	c, e := fb.Build()
	if e != nil {
		t.Errorf("Error %s", e.Error())
	} else {
		fmt.Printf("Result:\n%v\n", toolkit.JsonString(c))
	}
}

type employee struct {
	Id        string
	FirstName string
	LastName  string
	Age       int
}

// func TestHasSelect(t *testing.T) {
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("Unable to connect %s \n", e.Error())
// 	}
// 	defer c.Close()

// 	csr, e := c.NewQuery().Select().
// 		Cursor(nil)
// 	if e != nil {
// 		t.Errorf("Cursor pre error: %s \n", e.Error())
// 		return
// 	}
// 	if csr == nil {
// 		t.Errorf("Cursor not initialized")
// 		return
// 	}

// 	csr.Close()

// 	results := make([]map[string]interface{}, 0)
// 	e = csr.Fetch(&results, 2, false)
// 	if e != nil {
// 		t.Errorf("Unable to fetch N1: %s \n", e.Error())
// 	} else {
// 		fmt.Printf("Fetch N1 OK. Result: %v \n", results)
// 	}

// 	csr, e = c.NewQuery().Cursor(nil)
// 	if e != nil {
// 		t.Errorf("Cursor pre error: %s \n", e.Error())
// 		return
// 	}
// 	if csr == nil {
// 		t.Errorf("Cursor not initialized")
// 		return
// 	}

// 	e = csr.Fetch(&results, 3, false)
// 	if e != nil {
// 		t.Errorf("Unable to fetch N2: %s \n", e.Error())
// 	} else {
// 		fmt.Printf("Fetch N2 OK. Result: %v \n", results)
// 	}

// }

func TestSelect(t *testing.T) {
	t.Skip("Just Skip Test")
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	// csr, e := c.NewQuery().Select("Id", "FirstName", "LastName", "Age").Where(dbox.Startwith("FirstName", "Alip")).Cursor(nil)
	csr, e := c.NewQuery().Select("Id", "FirstName", "LastName", "Age").Where(dbox.Endwith("FirstName", "@consfirst")).
		Cursor(toolkit.M{}.Set("@confirst", "v"))
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
	// results := toolkit.M{}
	e = csr.Fetch(&results, 3, false)
	if e != nil {
		t.Errorf("Unable to fetch N1: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch N1 OK. Result: %v \n", results)
	}

	e = csr.Fetch(&results, 3, false)
	if e != nil {
		t.Errorf("Unable to fetch N2: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch N2 OK. Result: %v \n", results)
	}

	e = csr.ResetFetch()
	if e != nil {
		t.Errorf("Unable to reset fetch: %s \n", e.Error())
	}

	resultsstruct := make([]employee, 0)
	e = csr.Fetch(&resultsstruct, 5, false)
	if e != nil {
		t.Errorf("Unable to fetch N3: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch N3 OK. Result: %v \n", resultsstruct)
	}

	resultstruct := employee{}
	e = csr.Fetch(&resultstruct, 1, false)
	if e != nil {
		t.Errorf("Unable to fetch N3: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch N4 OK. Result: %v \n", resultstruct)
	}
}

func TestSelectLimit(t *testing.T) {
	t.Skip("Just Skip Test")
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().Select("Id", "FirstName").
		Take(3).Skip(0).
		Cursor(nil)
	if e != nil {
		t.Errorf("Cursor pre error: %s \n", e.Error())
		return
	}

	if csr == nil {
		t.Errorf("Cursor not initialized")
		return
	}

	resultsstruct := make([]employee, 0)
	e = csr.Fetch(&resultsstruct, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch N(0-3): %s \n", e.Error())
	} else {
		fmt.Printf("Record count(0-3) : %v \n", csr.Count())
		fmt.Printf("Fetch N(0-3) OK. Result: %v \n", resultsstruct)
	}

	csr.Close()

	csr, e = c.NewQuery().Select("Id", "FirstName").
		Skip(3).
		Cursor(nil)

	if e != nil {
		t.Errorf("Cursor pre error: %s \n", e.Error())
		return
	}

	if csr == nil {
		t.Errorf("Cursor not initialized")
		return
	}

	resultsstruct = make([]employee, 0)
	e = csr.Fetch(&resultsstruct, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch N(3-N): %s \n", e.Error())
	} else {
		fmt.Printf("Record count(3-N) : %v \n", csr.Count())
		fmt.Printf("Fetch N(3-N) OK. Result: %v \n", resultsstruct)
	}

	csr.Close()
}

func TestSelectCondition(t *testing.T) {
	t.Skip("Just Skip Test")
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().Select("Id", "LastName", "Age").
		Where(dbox.Contains("LastName", "m")).
		Take(10).Skip(0).
		Cursor(nil)
	if e != nil {
		t.Errorf("Cursor pre error: %s \n", e.Error())
		return
	}

	if csr == nil {
		t.Errorf("Cursor not initialized")
		return
	}

	resultsstruct := make([]employee, 0)
	e = csr.Fetch(&resultsstruct, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch N(0-10): %s \n", e.Error())
	} else {
		fmt.Printf("Record count(0-10) : %v \n", csr.Count())
		fmt.Printf("Fetch N(0-10) OK. Result: %v \n", resultsstruct)
	}

	csr.Close()

	csr, e = c.NewQuery().Select("Id", "LastName", "Age").
		Where(dbox.Contains("LastName", "m")).
		Take(10).Skip(10).
		Cursor(nil)
	if e != nil {
		t.Errorf("Cursor pre error: %s \n", e.Error())
		return
	}

	if csr == nil {
		t.Errorf("Cursor not initialized")
		return
	}

	resultsstruct = make([]employee, 0)
	e = csr.Fetch(&resultsstruct, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch N(10-20): %s \n", e.Error())
	} else {
		fmt.Printf("Record count(10-20) : %v \n", csr.Count())
		fmt.Printf("Fetch N(10-20) OK. Result: %v \n", resultsstruct)
	}

	csr.Close()
}

func TestSelectFreeQuery(t *testing.T) {
	t.Skip("Just Skip Test")
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	tq, e := dbox.NewQueryFromSQL(c, `SELECT Id, FirstName, LastName FROM tab WHERE ((Email = "userAA@yahoo.com" or Email = "userBB@yahoo.com") or (Email = "userCC@yahoo.com" or id = '12345'))`)
	if e != nil {
		t.Errorf("Query pre error: %s \n", e.Error())
		return
	}

	csr, e := tq.Cursor(nil)
	if e != nil {
		t.Errorf("Cursor pre error: %s \n", e.Error())
		return
	}
	if csr == nil {
		t.Errorf("Cursor not initialized")
		return
	}
	defer csr.Close()

	results := make([]toolkit.M, 0)
	e = csr.Fetch(&results, 3, false)
	if e != nil {
		t.Errorf("Unable to fetch N1: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch FN1 OK. Result: %v \n", results)
	}

	results = make([]toolkit.M, 0)
	e = csr.Fetch(&results, 2, false)
	if e != nil {
		t.Errorf("Unable to fetch N1: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch FN2 OK. Result: %v \n", results)
	}
}

// func TestSelectFilter(t *testing.T) {
// 	t.Skip("Just Skip Test")
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("Unable to connect %s \n", e.Error())
// 		return
// 	}
// 	defer c.Close()

// 	csr, e := c.NewQuery().
// 		Select("EmployeeId", "FirstName", "LastName", "Age").
// 		// Where(dbox.Eq("Age", "@age")).Cursor(toolkit.M{}.Set("age",15))
// 		// Where(dbox.Or(dbox.Eq("EmployeeId", "101-102-10"), dbox.Eq("EmployeeId", "101-102-3"), dbox.Eq("EmployeeId", "101-102-4"))).Cursor(nil)
// 		Where(dbox.And(dbox.Or(dbox.Eq("EmployeeId", "101-102-10"), dbox.Eq("EmployeeId", "101-102-3"), dbox.Eq("EmployeeId", "101-102-4")), dbox.Eq("Age", "30"))).Cursor(nil)
// 	if e != nil {
// 		t.Errorf("Cursor pre error: %s \n", e.Error())
// 		return
// 	}
// 	if csr == nil {
// 		t.Errorf("Cursor not initialized")
// 		return
// 	}
// 	defer csr.Close()

// 	ds, e := csr.Fetch(nil, 5, false)
// 	if e != nil {
// 		t.Errorf("Unable to fetch: %s \n", e.Error())
// 	} else {
// 		fmt.Printf("Fetch OK. Result: %v \n", ds.Data)
// 		// toolkit.JsonString(ds.Data))
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
	// t.Skip("Just Skip Test")
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()
	// ===================================================
	type employee struct {
		Id        string
		FirstName string
		LastName  string
		Age       string
		JoinDate  string
		Email     string
		Phone     string
	}

	data := employee{}
	data.Id = fmt.Sprintf("90012021")
	data.FirstName = fmt.Sprintf("Alip Sidik")
	data.LastName = fmt.Sprintf("Prayitno")
	data.Age = fmt.Sprintf("2C")
	data.JoinDate = fmt.Sprintf("2015-11-01")
	data.Email = fmt.Sprintf("user15@yahoo.com")
	data.Phone = fmt.Sprintf("085-XXX-XXX-XX")

	// data := toolkit.M{"name": "Save", "grade": 2}

	e = c.NewQuery().Save().Exec(toolkit.M{"data": data})
	if e != nil {
		t.Errorf("Unable to Insert: %s \n", e.Error())
	}

	// data = employee{}
	// data.Id = fmt.Sprintf("90012022")
	// data.FirstName = fmt.Sprintf("Test Name 01")
	// data.LastName = fmt.Sprintf("AA")
	// data.Age = fmt.Sprintf("2C")
	// data.JoinDate = fmt.Sprintf("2015-11-01")
	// data.Email = fmt.Sprintf("userAA@yahoo.com")
	// data.Phone = fmt.Sprintf("085-XXX-XXX-XX")

	// e = c.NewQuery().Insert().Exec(toolkit.M{"data": data})
	// if e != nil {
	// 	t.Errorf("Unable to Insert: %s \n", e.Error())
	// }
	// ===================================================

	// ===================================================
	/*dataupdate := toolkit.M{}.Set("Id", "90012019").Set("FirstName", "Alip").Set("LastName", "Sidik")

	e = c.NewQuery().Where(dbox.Eq("Id", "90012019")).Update().Exec(toolkit.M{"data": dataupdate})
	if e != nil {
		t.Errorf("Unable to update: %s \n", e.Error())
	}

	dataupdate = toolkit.M{}.Set("Id", "90012021").Set("FirstName", "Prayitno").Set("LastName", "Alip")

	e = c.NewQuery().Update().Exec(toolkit.M{"data": dataupdate})
	if e != nil {
		t.Errorf("Unable to update: %s \n", e.Error())
	}

	e = c.NewQuery().
		Delete().
		Where(dbox.Eq("Id", "90012020")).
		Exec(nil)

	if e != nil {
		t.Errorf("Unable to Delete: %s \n", e.Error())
	}*/
	// ===================================================

	// ===================================================
	/*q := c.NewQuery().SetConfig("multiexec", true).Save()

	for i := 1; i <= 5; i++ {
		datasave := toolkit.M{}.Set("Id", fmt.Sprintf("ID-%d", i+3)).Set("FirstName", fmt.Sprintf("BB-%d", i)).Set("LastName", fmt.Sprintf("BB-%d", i))
		datasave.Set("Email", "userAA@yahoo.com").Set("Phone", "XXX-0856")
		e = q.Exec(toolkit.M{
			"data": datasave,
		})
		if e != nil {
			t.Errorf("Unable to save: %s \n", e.Error())
		}
	}
	q.Close()*/

}
