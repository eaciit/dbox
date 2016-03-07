package xlsx

import (
	// "encoding/json"
	"fmt"
	"github.com/eaciit/dbox"
	"github.com/eaciit/toolkit"
	"testing"
)

func prepareConnection() (dbox.IConnection, error) {
	// mapHeader := make([]toolkit.M, 7)
	// mapHeader[0] = toolkit.M{}.Set("A", "date")
	// mapHeader[1] = toolkit.M{}.Set("B", "int")
	// mapHeader[2] = toolkit.M{}.Set("C", "int")
	// mapHeader[3] = toolkit.M{}.Set("D", "int")
	// mapHeader[4] = toolkit.M{}.Set("E", "int")
	// mapHeader[5] = toolkit.M{}.Set("F", "int")
	// mapHeader[6] = toolkit.M{}.Set("G", "int")
	// mapHeader := []toolkit.M{} //AddMap Header
	// var config = map[string]interface{}{}
	// var config = map[string]interface{}{"mapheader": mapHeader}

	config := toolkit.M{}.Set("rowstart", 5).Set("colsstart", 2).Set("useheader", true)
	ci := &dbox.ConnectionInfo{"E:\\data\\sample\\IO Price Indices.xlsm", "", "", "", config}
	c, e := dbox.NewConnection("xlsx", ci)
	if e != nil {
		return nil, e
	}

	e = c.Connect()
	if e != nil {
		return nil, e
	}

	return c, nil
}

// func TestConnect(t *testing.T) {
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("Unable to connect: %s \n", e.Error())
// 	}
// 	defer c.Close()
// }

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

	csr, e := c.NewQuery().Select("1", "2", "3", "4", "5").From("HIST").
		// Where(dbox.Contains("2", "183")).
		Where(dbox.Ne("1", "")).
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

	fmt.Println("Jumlah selected : ", csr.Count())

	results := make([]map[string]interface{}, 0)
	e = csr.Fetch(&results, 2, false)
	if e != nil {
		t.Errorf("Unable to fetch N1: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch N1 OK. Result: %v \n", results)
	}

	// e = csr.Fetch(&results, 6, false)
	// if e != nil {
	// 	t.Errorf("Unable to fetch N2: %s \n", e.Error())
	// } else {
	// 	fmt.Printf("Fetch N2 OK. Result: %v \n", results)
	// }

	// e = csr.ResetFetch()
	// if e != nil {
	// 	t.Errorf("Unable to reset fetch: %s \n", e.Error())
	// }

	// ds, e = csr.Fetch(nil, 5, false)
	// if e != nil {
	// 	t.Errorf("Unable to fetch N3: %s \n", e.Error())
	// } else {
	// 	fmt.Printf("Fetch N3 OK. Result: %v \n", ds.Data)
	// }
}

// func TestSelectFilter(t *testing.T) {
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("Unable to connect %s \n", e.Error())
// 		return
// 	}
// 	defer c.Close()

// 	csr, e := c.NewQuery().
// 		Select("EmployeeId", "FirstName", "LastName", "Age").
// 		// Where(dbox.Eq("EmployeeId", "101-102-4")).Cursor(nil)
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

// func TestCRUD(t *testing.T) {
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("Unable to connect %s \n", e.Error())
// 		return
// 	}
// 	defer c.Close()

// 	type employee struct {
// 		EmployeeId string
// 		FirstName  string
// 		LastName   string
// 		Age        string
// 		JoinDate   string
// 		Email      string
// 		Phone      string
// 	}

// 	data := employee{}
// 	data.EmployeeId = fmt.Sprintf("90012019")
// 	data.FirstName = fmt.Sprintf("Alip Sidik")
// 	data.LastName = fmt.Sprintf("Prayitno")
// 	data.Age = fmt.Sprintf("2C")
// 	data.JoinDate = fmt.Sprintf("2015-11-01")
// 	data.Email = fmt.Sprintf("user15@yahoo.com")
// 	data.Phone = fmt.Sprintf("085-XXX-XXX-XX")

// 	e = c.NewQuery().Insert().Exec(toolkit.M{"data": data})
// 	if e != nil {
// 		t.Errorf("Unable to Insert: %s \n", e.Error())
// 	}

// 	// dataStr := []string{"90013012", "AABBCC", "DDEEFF", "10", "2015-11-01", "AABB@CC.com"}
// 	// e = c.NewQuery().Insert().Exec(toolkit.M{"data": dataStr})
// 	// if e != nil {
// 	// 	t.Errorf("Unable to Insert: %s \n", e.Error())
// 	// }

// 	// dataJson := `{
// 	// 	"EmployeeId": "901-999-1",
// 	// 	"FirstName": "Quail",
// 	// 	"LastName": "Boyd",
// 	// 	"Email": "adipiscing.lacus@diamdictum.ca"
// 	// }`

// 	// e = c.NewQuery().Insert().Exec(toolkit.M{"data": dataJson})
// 	// if e != nil {
// 	// 	t.Errorf("Unable to Insert: %s \n", e.Error())
// 	// }

// 	data = employee{}
// 	data.FirstName = fmt.Sprintf("Alip")
// 	data.LastName = fmt.Sprintf("Sidik")
// 	data.Age = fmt.Sprintf("2X")
// 	data.JoinDate = fmt.Sprintf("1990-11-01")
// 	data.Email = fmt.Sprintf("user15@gmail.com")
// 	data.Phone = fmt.Sprintf("085-0000-0000")

// 	e = c.NewQuery().Where(dbox.Eq("EmployeeId", "90012019")).Update().Exec(toolkit.M{"data": data})
// 	if e != nil {
// 		t.Errorf("Unable to update: %s \n", e.Error())
// 	}

// 	// e = c.NewQuery().
// 	// 	Delete().
// 	// 	Where(dbox.Eq("EmployeeId", "90012019")).
// 	// 	Exec(nil)
// 	// if e != nil {
// 	// 	t.Errorf("Unable to Delete: %s \n", e.Error())
// 	// }

// 	// e = c.NewQuery().From("testtables").Delete().Exec(nil)
// 	// if e != nil {
// 	// 	t.Errorf("Unablet to clear table %s\n", e.Error())
// 	// 	return
// 	// }

// 	// q := c.NewQuery().SetConfig("multiexec", true).From("testtables").Save()
// 	// type user struct {
// 	// 	Id    string `bson:"_id"`
// 	// 	Title string
// 	// 	Email string
// 	// }
// 	// for i := 1; i <= 10000; i++ {
// 	// 	//go func(q dbox.IQuery, i int) {
// 	// 	data := user{}
// 	// 	data.Id = fmt.Sprintf("User-%d", i)
// 	// 	data.Title = fmt.Sprintf("User-%d's name", i)
// 	// 	data.Email = fmt.Sprintf("User-%d@myco.com", i)
// 	// 	if i == 10 || i == 20 || i == 30 {
// 	// 		data.Email = fmt.Sprintf("User-%d@myholding.com", i)
// 	// 	}
// 	// 	e = q.Exec(toolkit.M{
// 	// 		"data": data,
// 	// 	})
// 	// 	if e != nil {
// 	// 		t.Errorf("Unable to save: %s \n", e.Error())
// 	// 	}
// 	// }
// 	// q.Close()

// 	// data 		:= user{}
// 	// data.Id 	= fmt.Sprintf("User-15")
// 	// data.Title 	= fmt.Sprintf("User Lima Belas")
// 	// data.Email 	= fmt.Sprintf("user15@yahoo.com")
// 	// e = c.NewQuery().From("testtables").Update().Exec(toolkit.M{"data": data})
// 	// if e != nil {
// 	// 	t.Errorf("Unable to update: %s \n", e.Error())
// 	// }
// }
