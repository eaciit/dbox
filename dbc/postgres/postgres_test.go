package postgres

import ( 
	"github.com/eaciit/dbox"
	"github.com/eaciit/toolkit"   
	"testing"
	"fmt"
	"time"
)

func prepareConnection() (dbox.IConnection, error) {
	ci := &dbox.ConnectionInfo{"localhost:5432", "eccolony", "postgres", "12345", nil}
	c, e := dbox.NewConnection("postgres", ci) 
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



// func TestCRUD(t *testing.T) {
// 	//t.Skip()
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("Unable to connect %s \n", e.Error())
// 		return
// 	}
// 	defer c.Close()
	 
// 	q := c.NewQuery().SetConfig("multiexec", true).From("tes").Save()
// 	type user struct {
// 		Id     int   
// 		Name   string 
// 		Date   time.Time 
// 	}
	 
// 		//go func(q dbox.IQuery, i int) {
// 		data := user{}
// 		data.Id = 22444
// 		data.Name = "dsad2"
// 		data.Date = time.Now().UTC()

// 		e = q.Exec(toolkit.M{
// 			"data": data,
// 		})
// 		if e != nil {
// 			t.Errorf("Unable to save: %s \n", e.Error())
// 		}
	 
// }


 

// func TestSelect(t *testing


// func TestSelectAggregate(t *testing.T) {
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("Unable to connect %s \n", e.Error())
// 	}
// 	defer c.Close()

// 	fb := c.Fb()
// 	csr, e := c.NewQuery().
		//Select("_id", "email").
		//Where(c.Fb().Eq("email", "arief@eaciit.com")).
	// 	Aggr(dbox.AggSum, 1, "Count").
	// 	Aggr(dbox.AggSum, 1, "Avg").
	// 	From("appusers").
	// 	Group("").
	// 	Cursor(nil)
	// if e != nil {
	// 	t.Errorf("Cursor pre error: %s \n", e.Error())
	// 	return
	// }
	// if csr == nil {
	// 	t.Errorf("Cursor not initialized")
	// 	return
	// }
	// defer csr.Close()

	//rets := []toolkit.M{}

// 	ds, e := csr.Fetch(nil, 0, false)
// 	if e != nil {
// 		t.Errorf("Unable to fetch: %s \n", e.Error())
// 	} else {
// 		fmt.Printf("Fetch OK. Result: %v \n",
// 			toolkit.JsonString(ds.Data[0]))

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
// 		Where(dbox.Or(dbox.Eq("id", "22"),(dbox.Eq("name","a")))).
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

func TestCRUD(t *testing.T) {
	// t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()
// 	// e = c.NewQuery().From("tes").Delete().Exec(nil)
// 	// if e != nil {
// 	// 	t.Errorf("Unablet to clear table %s\n", e.Error())
// 	// 	return
// 	// }
// 	// e = c.NewQuery().From("testtables").Where(dbox.And(dbox.Eq("id", "3"),dbox.Eq("title", "user3"))).Delete().Exec(nil)
// 	// if e != nil {
// 	// 	t.Errorf("Unablet to delete table %s\n", e.Error())
// 	// 	return
// 	// }
// 	// defer c.Close()
	q := c.NewQuery().SetConfig("multiexec", true).From("tes").Save()
	type user struct {
		Id     string   
		Name   string
		Date   time.Time 
	}
	 
// 	// 	//go func(q dbox.IQuery, i int) {
		data := user{}
		data.Id = "2222222"
		data.Name = "muhammad"  
		data.Date = time.Now().UTC()

// 		fmt.Println("data >>",data)
		e = q.Exec(toolkit.M{
			"data": data,
		})
		e = q.Exec(toolkit.M{
			"data": data,
		})
		if e != nil {
			t.Errorf("Unable to save: %s \n", e.Error())
		}
	 	defer c.Close()
}
// 	// // q.Close() 
// 	// data.Id = "3" 
// 	// data.Name = "uuuuuuuuuu"
// 	// e = c.NewQuery().From("testtables").Where(dbox.Eq("id", "3")).Update().Exec(toolkit.M{"data": data})
// 	// if e != nil {
// 	// 	t.Errorf("Unable to update: %s \n", e.Error())
// 	// }
	 
// }

 