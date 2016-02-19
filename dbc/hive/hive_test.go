package hive

import (
	"fmt"
	"github.com/eaciit/dbox"
	"github.com/eaciit/toolkit"
	"testing"
)

type Sample7 struct {
	Code        string  `tag_name:"code"`
	Description string  `tag_name:"description"`
	Total_emp   float64 `tag_name:"total_emp"`
	Salary      float64 `tag_name:"salary"`
}

type Students struct {
	Name    string `tag_name:"name"`
	Age     int    `tag_name:"age"`
	Phone   string `tag_name:"phone"`
	Address string `tag_name:"address"`
}

func prepareConnection() (dbox.IConnection, error) {
	// ci := &dbox.ConnectionInfo{"192.168.0.223:10000", "default", "developer", "b1gD@T@", nil}
	ci := &dbox.ConnectionInfo{"192.168.0.223:10000", "default", "hdfs", "", nil}
	c, e := dbox.NewConnection("hive", ci)
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
	_, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect: %s \n", e.Error())
	}
}

func TestSelect(t *testing.T) {
	//t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Select("name", "age", "phone").
		From("students").
		// Where(dbox.Eq("name", "Alexis Sanchez")).
		// Where(dbox.Gt("age", 25)).
		// Where(dbox.Gte("age", 25)).
		// Where(dbox.Lt("age", 25)).
		// Where(dbox.Lte("age", 25)).
		// Where(dbox.In("name", "cakep", "orang gile")).
		// Where(dbox.In("age", 23, 45)).
		// Where(dbox.Nin("age", 23, 45)).
		// Where(dbox.And(dbox.Gt("age", 25), dbox.Eq("name", "Keanu Rives"))).
		// Where(dbox.Contains("name", "Al", "an")).
		// Where(dbox.Or(dbox.Contains("name", "re"), dbox.Contains("name", "an"))).
		// Where(dbox.Startwith("name", "Ro")).
		Where(dbox.Endwith("name", "es")).
		// Order("name").
		// Skip(2).
		// Take(5).
		Cursor(nil)
	// Where(dbox.In("name", "@name1", "@name2")).
	// Cursor(toolkit.M{}.Set("name1", "clyne").Set("name2", "Kane"))
	// Where(dbox.Lte("tanggal", "@date")).
	// Cursor(toolkit.M{}.Set("date", tanggal1))
	// Where(dbox.Eq("name", "@nama")).
	// Cursor(toolkit.M{}.Set("nama", "clyne"))
	// Where(dbox.Eq("umur", "@age")).
	// Cursor(toolkit.M{}.Set("age", 25))
	// Where(dbox.And(dbox.Gt("umur", "@age"), dbox.Eq("name", "@nama"))).
	// Cursor(toolkit.M{}.Set("age", 25).Set("nama", "Kane"))
	// Where(dbox.And(dbox.Or(dbox.Eq("name", "@name1"), dbox.Eq("name", "@name2"),
	// dbox.Eq("name", "@name3")), dbox.Lt("umur", "@age"))).
	// Cursor(toolkit.M{}.Set("name1", "Kane").Set("name2", "Roy").
	// Set("name3", "Oscar").Set("age", 30))
	// Where(dbox.And(dbox.Or(dbox.Eq("name", "@name1"), dbox.Eq("name", "@name2"),
	// dbox.Eq("name", "@name3")), dbox.Lt("umur", "@age"))).
	// Cursor(toolkit.M{}.Set("name1", "Kane").Set("name2", "Roy").
	// Set("name3", "Oscar").Set("age", 30))

	if e != nil {
		t.Errorf("Cursor pre error: %s \n", e.Error())
		return
	}
	if csr == nil {
		t.Errorf("Cursor not initialized")
		return
	}
	defer csr.Close()

	results := make([]map[string]interface{}, 0) //[]Sample7{}
	err := csr.Fetch(&results, 0, false)
	if err != nil {
		t.Errorf("Unable to fetch: %s \n", err.Error())
	} else {
		fmt.Println("======================")
		fmt.Println("Select with LIMIT")
		fmt.Println("======================")

		fmt.Printf("Fetch limit 5 OK. Result:\n")
		fmt.Printf("%v \n", toolkit.JsonString(results))
	}
}

func TestFetch(t *testing.T) {
	// t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Select("code", "description", "total_emp", "salary").
		From("sample_07").
		// Where(dbox.Eq("name", "Bourne")).
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

	results := make([]map[string]interface{}, 0) //[]Sample7{}
	err := csr.Fetch(&results, 2, false)
	if err != nil {
		t.Errorf("Unable to fetch: %s \n", err.Error())
	} else {
		fmt.Println("======================")
		fmt.Println("Select with FETCH")
		fmt.Println("======================")

		fmt.Printf("Fetch N2 OK. Result:%v \n", toolkit.JsonString(results))
	}
}

func TestSelectAggregate(t *testing.T) {
	// t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close() //temporary unused

	csr, e := c.NewQuery().
		Select("nama").
		// Aggr(dbox.AggrSum, "nama", "Total_Item").
		Aggr(dbox.AggrMax, "amount", "MaxAmount").
		Aggr(dbox.AggrSum, "amount", "TotalAmount").
		Aggr(dbox.AggrAvr, "amount", "AverageAmount").
		From("orders").
		Group("nama").
		// Order("nama").
		// Skip(2).
		// Take(1).
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
		fmt.Println("======================")
		fmt.Println("QUERY AGGREGATION")
		fmt.Println("======================")
		for _, val := range results {
			fmt.Printf("Fetch N OK. Result: %v \n",
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

	q := c.NewQuery().SetConfig("multiexec", true).From("students").Insert()
	dataInsert := Students{}
	dataInsert.Name = "aje buset dah"
	dataInsert.Age = 45
	dataInsert.Phone = "mau tau aja!!"
	dataInsert.Address = "mau tau aja!!"

	e = q.Exec(toolkit.M{"data": dataInsert})
	if e != nil {
		t.Errorf("Unable to insert data : %s \n", e.Error())
	} else {
		fmt.Println("======================")
		fmt.Println("Test Insert OK")
		fmt.Println("======================")
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

	/*=============================== with condition and data ===============================*/

	data := Students{}
	// data.Id = "7"
	data.Name = "busyet"
	data.Age = 20
	data.Phone = "24"
	data.Address = "alamat palsu"
	e = c.NewQuery().From("students").Where(dbox.Eq("name", "aje buset dah")).Update().Exec(toolkit.M{"data": data})
	if e != nil {
		t.Errorf("Unable to update: %s \n", e.Error())
	} else {
		fmt.Println("======================")
		fmt.Println("Test Update OK")
		fmt.Println("======================")
	}

	/* ===============================with config=============================== */
	// data := Students{}
	// // data.Id = "7"
	// data.Name = "busyet"
	// data.Age = 20
	// data.Phone = "24"
	// e = c.NewQuery().SetConfig("multiexec", false).From("students").Update().Exec(toolkit.M{"data": data})
	// if e != nil {
	// 	t.Errorf("Unable to update: %s \n", e.Error())
	// }
}

func TestDelete(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	e = c.NewQuery().From("students").Where(dbox.And(dbox.Eq("name", "dwayne johnson"), dbox.Eq("age", 32))).Delete().Exec(nil)
	// e = c.NewQuery().From("students").Where(dbox.Eq("name", "dwayne johnson")).Delete().Exec(nil)
	if e != nil {
		t.Errorf("Unable to delete table %s\n", e.Error())
		return
	}

	/* ===============================CLEAR ALL TABLE DATA==============================*/

	// e = c.NewQuery().SetConfig("multiexec", true).
	// 	From("coba").Delete().Exec(nil)
	// if e != nil {
	// 	t.Errorf("Unable to clear table %s\n", e.Error())
	// 	return
	// }
}

func TestSave(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	/* ===============================SAVE DATA============================== */
	// q := c.NewQuery().SetConfig("multiexec", false).From("coba").Save()
	// dataInsert := Coba{}
	// dataInsert.Id = fmt.Sprintf("1")
	// dataInsert.Name = fmt.Sprintf("multi, with data contains ID, update")

	q := c.NewQuery().SetConfig("multiexec", false).From("students").Save()
	dataInsert := Students{}
	dataInsert.Name = "Sergio Aguero"
	dataInsert.Age = 27
	dataInsert.Phone = "031947499"

	e = q.Exec(toolkit.M{"data": dataInsert})
	if e != nil {
		t.Errorf("Unable to insert data : %s \n", e.Error())
	}
}
