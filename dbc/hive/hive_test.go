package hive

import (
	"fmt"
	"github.com/eaciit/dbox"
	"github.com/eaciit/toolkit"
	"testing"
)

type Sample7 struct {
	Code        string `tag_name:"code"`
	Description string `tag_name:"description"`
	Total_emp   string `tag_name:"total_emp"`
	Salary      string `tag_name:"salary"`
}

type Students struct {
	Name  string `tag_name:"name"`
	Age   int    `tag_name:"age"`
	Phone string `tag_name:"phone"`
}

func prepareConnection() (dbox.IConnection, error) {
	ci := &dbox.ConnectionInfo{"192.168.0.223:10000", "default", "developer", "b1gD@T@", nil}
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
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}

	csr, e := c.NewQuery().
		Select("code", "description", "total_emp", "salary").
		From("sample_07").
		Take(5).
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

	results := []Sample7{}
	err := csr.Fetch(&results, 0, false)
	if err != nil {
		t.Errorf("Unable to fetch: %s \n", err.Error())
	} else {
		fmt.Println("======================")
		fmt.Println("Select with FILTER")
		fmt.Println("======================")

		fmt.Printf("Fetch limit 5 OK. Result:\n")
		fmt.Printf("%v \n", toolkit.JsonString(results))
	}
}

func TestFetch(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}

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

	results := []Sample7{}
	err := csr.Fetch(&results, 2, false)
	if err != nil {
		t.Errorf("Unable to fetch: %s \n", err.Error())
	} else {
		fmt.Printf("Fetch N2 OK. Result:%v \n", toolkit.JsonString(results))
	}
}

func TestSelectAggregate(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Select("name").
		Aggr(dbox.AggrSum, "age", "TotalItem").
		// Aggr(dbox.AggrMax, "age", "MaxAge").
		// Aggr(dbox.AggrAvr, "age", "AverageAge").
		From("students").
		Group("name").
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

func TestCRUD(t *testing.T) {
	//t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}

	// ===============================INSERT==============================
	q := c.NewQuery().SetConfig("multiexec", true).From("students").Insert()
	dataInsert := Students{}
	dataInsert.Name = "John Terry"
	dataInsert.Age = 45
	dataInsert.Phone = "0856768686"

	e = q.Exec(toolkit.M{"data": dataInsert})
	if e != nil {
		t.Errorf("Unable to insert data : %s \n", e.Error())
	}

	// ===============================INSERT MANY==============================
	// q := c.NewQuery().SetConfig("multiexec", true).From("tes").Insert()
	// nama := []string{"Barkley", "Vidal", "Arnautovic", "Agger", "Wijnaldum", "Ighalo", "Mahrez"}
	// dataInsert := User{}

	// for i, val := range nama {

	// 	dataInsert.Id = strconv.Itoa(i + 1)
	// 	dataInsert.Name = fmt.Sprintf(val)
	// 	dataInsert.Tanggal = time.Now()
	// 	dataInsert.Umur = i + 20
	// 	e = q.Exec(toolkit.M{
	// 		"data": dataInsert,
	// 	})
	// 	if e != nil {
	// 		t.Errorf("Unable to save: %s \n", e.Error())
	// 	}
	// }

	/* ===============================SAVE DATA============================== */
	// q := c.NewQuery().SetConfig("multiexec", false).From("coba").Save()
	// dataInsert := Coba{}
	// dataInsert.Id = fmt.Sprintf("1")
	// dataInsert.Name = fmt.Sprintf("multi, with data contains ID, update")

	// q := c.NewQuery().SetConfig("multiexec", false).From("NoID").Save()
	// dataInsert := NoID{}
	// dataInsert.Aidi = fmt.Sprintf("30")
	// dataInsert.Name = fmt.Sprintf("no multi, with data contains no ID")

	// e = q.Exec(toolkit.M{"data": dataInsert})
	// if e != nil {
	// 	t.Errorf("Unable to insert data : %s \n", e.Error())
	// }

	/* ===============================UPDATE============================== */

	// data := User{}
	// data.Id = "7"
	// data.Name = "Oscar"
	// data.Tanggal = time.Now()
	// data.Umur = 24
	// e = c.NewQuery().From("tes").Where(dbox.Eq("id", "7")).Update().Exec(toolkit.M{"data": data})
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

	/* with config */
	// data := Coba{}
	// data.Id = "2"
	// data.Name = "false, no where, with data 2"
	// e = c.NewQuery().SetConfig("multiexec", false).From("coba").Update().Exec(toolkit.M{"data": data})
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
	// e = c.NewQuery().From("coba").Where(dbox.And(dbox.Eq("id", "2"), dbox.Eq("name", "Thuram"))).Delete().Exec(nil)
	// if e != nil {
	// 	t.Errorf("Unable to delete table %s\n", e.Error())
	// 	return
	// }

	// ===============================CLEAR ALL TABLE DATA==============================

	// e = c.NewQuery().SetConfig("multiexec", true).
	// 	From("coba").Delete().Exec(nil)
	// if e != nil {
	// 	t.Errorf("Unable to clear table %s\n", e.Error())
	// 	return
	// }
}
