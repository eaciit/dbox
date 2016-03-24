package hive

import (
	"fmt"
	"github.com/eaciit/dbox"
	//_ "github.com/eaciit/dbox/dbc/hive"
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
	//ci := &dbox.ConnectionInfo{"192.168.0.223:10000", "default", "developer", "b1gD@T@", nil}
	ci := &dbox.ConnectionInfo{"192.168.0.223:10000", "default", "hdfs", "", toolkit.M{}.Set("path", "").Set("delimiter", "tsv")}
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
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Select("_id", "nama", "quantity", "price", "amount").
		From("orders").
		// Where(dbox.Eq("nama", "buku")).
		// Where(dbox.Ne("nama", "buku")).
		// Where(dbox.Gt("price", 100000)).
		// Where(dbox.Gte("price", 100000)).
		// Where(dbox.Lt("price", 100000)).
		// Where(dbox.Lte("price", 100000)).
		// Where(dbox.In("nama", "tas", "dompet")).
		// Where(dbox.Nin("nama", "tas", "dompet")).
		// Where(dbox.And(dbox.Gt("amount", 100000), dbox.Eq("nama", "buku"))).
		// Where(dbox.Contains("nama", "tem", "pe")).
		// Where(dbox.Or(dbox.Contains("nama", "bu"), dbox.Contains("nama", "do"))).
		// Where(dbox.Startwith("nama", "bu")).
		// Where(dbox.Endwith("nama", "as")).
		// Order("nama").
		// Skip(2).
		// Take(5).
		Cursor(nil)
	// Where(dbox.In("nama", "@name1", "@name2")).
	// Cursor(toolkit.M{}.Set("@name1", "stempel").Set("@name2", "buku"))
	// Where(dbox.Lte("price", "@price")).
	// Cursor(toolkit.M{}.Set("@price", 100000))
	// Where(dbox.Eq("nama", "@nama")).
	// Cursor(toolkit.M{}.Set("@nama", "tas"))
	// Where(dbox.Eq("price", "@price")).
	// Cursor(toolkit.M{}.Set("@price", 200000))
	// Where(dbox.And(dbox.Gt("price", "@price"), dbox.Eq("status", "@status"))).
	// Cursor(toolkit.M{}.Set("@price", 100000).Set("@status", "available"))
	// Where(dbox.And(dbox.Or(dbox.Eq("nama", "@name1"), dbox.Eq("nama", "@name2"),
	// dbox.Eq("nama", "@name3")), dbox.Lt("quantity", "@quantity"))).
	// Cursor(toolkit.M{}.Set("@name1", "buku").Set("@name2", "tas").
	// Set("@name3", "dompet").Set("@quantity", 4))
	// Where(dbox.Or(dbox.Or(dbox.Eq("nama", "@name1"), dbox.Eq("nama", "@name2"),
	// dbox.Eq("nama", "@name3")), dbox.Gt("quantity", "@quantity"))).
	// Cursor(toolkit.M{}.Set("@name1", "buku").Set("@name2", "tas").
	// Set("@name3", "dompet").Set("@quantity", 3))

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
	t.Skip()
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

func TestTakeSkip(t *testing.T) {
	//t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Select("code", "description", "total_emp", "salary").
		From("sample_07").
		//Take(10).
		Skip(30).
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
	err := csr.Fetch(&results, 0, false)
	if err != nil {
		t.Errorf("Unable to fetch: %s \n", err.Error())
	} else {
		fmt.Println("=========================")
		fmt.Println("Select with Take and Skip")
		fmt.Println("=========================")

		fmt.Printf("Fetch N2 OK. Result:%v \n", toolkit.JsonString(results))
	}
}

func TestSelectAggregate(t *testing.T) {
	t.Skip()
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
	dataInsert.Name = "zz top"
	dataInsert.Age = 45
	dataInsert.Phone = "+62856"
	dataInsert.Address = "sesame street"

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
