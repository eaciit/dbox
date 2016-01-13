package mssql

import ( 
	"github.com/eaciit/dbox"
	"github.com/eaciit/toolkit"   
	"testing"
	"fmt"
	// "time" 
)

func prepareConnection() (dbox.IConnection, error){  
	ci := &dbox.ConnectionInfo{"localhost", "eccolony", "sa", "Password.Sql", nil}
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
	}else{
		// fmt.Println(c)
	}
	defer c.Close()
}
 

func TestSelect(t *testing.T) {
	c, e := prepareConnection()

	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()
    
	// csr, e := c.NewQuery().Select().From("tes").Where(dbox.Eq("id", "3")).Cursor(nil) 
	csr, e := c.NewQuery().Select("id","name","date").From("tes").Cursor(nil) 
	
	if e != nil {
		t.Errorf("Cursor pre error: %s \n", e.Error())
		return
	}
	if csr == nil {
		t.Errorf("Cursor not initialized")
		return
	}
	defer csr.Close()

	// //rets := []toolkit.M{}
	 
	ds, e := csr.Fetch(nil, 1, false)
	if e != nil {
		t.Errorf("Unable to fetch all: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch N OK. Result: %v \n",
			ds.Data)
	}

	e = csr.ResetFetch()
	if e != nil {
		t.Errorf("Unable to reset fetch: %s \n", e.Error())
	}

	ds, e = csr.Fetch(nil, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch N: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch N OK. Result: %v \n",
			ds.Data)
	}
}

func TestSelectFilter(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Select("id", "name").
		Where(dbox.And(dbox.Eq("id", "1"),(dbox.Eq("name","a")))).
		From("tes").Cursor(nil)
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
		fmt.Printf("Fetch N OK. Result: %v \n",
			ds.Data)
	}
}

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
	 
	e = c.NewQuery().From("tes").Where(dbox.And(dbox.Eq("id", "1133331"),dbox.Eq("name", "testing"))).Delete().Exec(nil)
	if e != nil {
		t.Errorf("Unablet to delete table %s\n", e.Error())
		return
	}
	defer c.Close()

	e = c.NewQuery().From("tes").Delete().Exec(nil)
	if e != nil {
		t.Errorf("Unablet to clear table %s\n", e.Error())
		return
	}

	defer c.Close()
	 
	// q := c.NewQuery().SetConfig("multiexec", true).From("tes").Save()
	type user struct {
		// Id     int   
		Name   string 
		// Date   time.Time 
	}
	 
	// // 	//go func(q dbox.IQuery, i int) {
		data := user{}
		// data.Id =111		
		data.Name = "testingupdate2222"
		// data.Date = time.Now()
  
  		// e = q.Exec(toolkit.M{
		// 	"data": data,
		// })
		// if e != nil {
		// 	t.Errorf("Unable to save: %s \n", e.Error())
		// }
		 e = c.NewQuery().From("tes").Where(dbox.Eq("id", "111")).Update().Exec(toolkit.M{"data": data})
		if e != nil {
			t.Errorf("Unable to update: %s \n", e.Error())
		}
	 
}

