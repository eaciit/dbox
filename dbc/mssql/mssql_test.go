package mssql

import (
	"fmt"
	"github.com/eaciit/dbox"
	"github.com/eaciit/toolkit"
	"testing"
	"time"
)

type User struct {
	Player_id string
	Nama      string
	Tanggal   time.Time
	Umur      int
}

type Player struct {
	Player_id string
	Nama      string
	Umur      int
}

type UpdateID struct {
	Player_id string
}

func prepareConnection() (dbox.IConnection, error) {
	//ci := &dbox.ConnectionInfo{"localhost", "Tes", "sa", "Password.Sql", nil}
	// ci := &dbox.ConnectionInfo{"localhost", "test", "sa", "budi123", nil}
	ci := &dbox.ConnectionInfo{"localhost", "test", "Lenovo-PC\\Lenovo Z40", "123456", nil}
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

// 	//csr, e := c.NewQuery().Select().From("tes").Where(dbox.Eq("player_id", "3")).Cursor(nil)
// 	csr, e := c.NewQuery().Select("player_id", "nama").From("tes").Cursor(nil)

// 	if e != nil {
// 		t.Errorf("Cursor pre error: %s \n", e.Error())
// 		return
// 	}
// 	if csr == nil {
// 		t.Errorf("Cursor not initialized")
// 		return
// 	}
// 	defer csr.Close()

// 	results := make([]map[string]interface{}, 0)

// 	err := csr.Fetch(&results, 0, false)
// 	if err != nil {
// 		t.Errorf("Unable to fetch all: %s \n", err.Error())
// 	} else {
// 		fmt.Println("=========================")
// 		fmt.Println("Select with NO filter")
// 		fmt.Println("=========================")

// 		for _, val := range results {
// 			fmt.Printf("Fetch N OK. Result: %v \n",
// 				toolkit.JsonString(val))
// 		}
// 	}

// 	e = csr.ResetFetch()
// 	if e != nil {
// 		t.Errorf("Unable to reset fetch: %s \n", e.Error())
// 	}
// }

func TestProcedure(t *testing.T) {
	c, _ := prepareConnection()
	defer c.Close()

	// csr, e := c.NewQuery().Command("procedure", toolkit.M{}.Set("name", "getUmur").Set("parms", toolkit.M{}.Set("@nama", "Vidal"))).Cursor(nil)
	csr, e := c.NewQuery().Command("procedure", toolkit.M{}.Set("name", "getUmurIn").Set("parms", toolkit.M{}.Set("@umur1", "20").Set("@umur2", "23"))).Cursor(nil)
	// csr, e := c.NewQuery().Command("procedure", toolkit.M{}.Set("name", "getUmurIn").
	// 	Set("parms", toolkit.M{}.Set("@p_umur1", "20").Set("@p_umur2", "23").
	// 	Set("@@o_nama", "varchar2").Set("@@o_umur", "number"))).Cursor(nil)
	if csr == nil {
		t.Errorf("Cursor not initialized", e.Error())
		return
	}
	defer csr.Close()

	results := make([]map[string]interface{}, 0)

	err := csr.Fetch(&results, 0, false)
	fmt.Println("Hasil Procedure : ", results)
	if err != nil {
		t.Errorf("Unable to fetch: %s \n", err.Error())
	} else {
		fmt.Println("======================")
		fmt.Println("STORED PROCEDURE")
		fmt.Println("======================")
		for _, val := range results {
			fmt.Printf("Fetch N OK. Result: %v \n",
				toolkit.JsonString(val))
		}
	}
}

func TestSelectFilter(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	layoutFormat := "2006-01-02 15:04:05"
	dateValue1 := "2016-01-12 14:35:54"
	dateValue2 := "2016-01-12 14:36:15"
	var tanggal1 time.Time
	var tanggal2 time.Time
	tanggal1, _ = time.Parse(layoutFormat, dateValue1)
	tanggal2, _ = time.Parse(layoutFormat, dateValue2)
	fmt.Println(tanggal1, tanggal2)

	csr, e := c.NewQuery().
		Select("player_id", "nama", "tanggal", "umur").
		From("tes").
		// Where(dbox.Eq("nama", "Bourne")).
		// Where(dbox.Ne("nama", "Bourne")).
		// Where(dbox.Gt("umur", 25)).
		Where(dbox.Endwith("nama", "ey")).
		// Where(dbox.Gte("umur", 25)).
		// Where(dbox.Lt("umur", 25)).
		// Where(dbox.Lte("tanggal", tanggal1)).
		// Where(dbox.Lte("umur", 25)).
		// Where(dbox.In("nama", "vidal", "bourne")).
		// Where(dbox.In("umur", 25, 30)).
		// Where(dbox.Nin("umur", 25, 30)).
		// Where(dbox.In("tanggal", tanggal1, tanggal2)).
		// Where(dbox.And(dbox.Gt("umur", 25), dbox.Eq("nama", "Roy"))).
		// Where(dbox.Contains("nama", "ar", "ov")).
		Order("nama").
		// Skip(2).
		Take(1).
		Cursor(nil)
	// Where(dbox.In("nama", "@nama1", "@nama2")).
	// Cursor(toolkit.M{}.Set("nama1", "clyne").Set("nama2", "Kane"))
	// Where(dbox.Lte("tanggal", "@date")).
	// Cursor(toolkit.M{}.Set("date", tanggal1))
	// Where(dbox.Eq("nama", "@nama")).
	// Cursor(toolkit.M{}.Set("nama", "clyne"))
	// Where(dbox.Eq("umur", "@age")).
	// Cursor(toolkit.M{}.Set("age", 25))
	// Where(dbox.And(dbox.Gt("umur", "@age"), dbox.Eq("nama", "@nama"))).
	// Cursor(toolkit.M{}.Set("age", 25).Set("nama", "Kane"))
	// Where(dbox.And(dbox.Or(dbox.Eq("nama", "@nama1"), dbox.Eq("nama", "@nama2"),
	// dbox.Eq("nama", "@nama3")), dbox.Lt("umur", "@age"))).
	// Cursor(toolkit.M{}.Set("nama1", "Kane").Set("nama2", "Roy").
	// Set("nama3", "Oscar").Set("age", 30))
	// Where(dbox.And(dbox.Or(dbox.Eq("nama", "@nama1"), dbox.Eq("nama", "@nama2"),
	// dbox.Eq("nama", "@nama3")), dbox.Lt("umur", "@age"))).
	// Cursor(toolkit.M{}.Set("nama1", "Kane").Set("nama2", "Roy").
	// Set("nama3", "Oscar").Set("age", 30))

	if e != nil {
		t.Errorf("Cursor pre error: %s \n", e.Error())
		return
	}
	if csr == nil {
		t.Errorf("Cursor not initialized")
		return
	}
	defer csr.Close()

	// results := make([]map[string]interface{}, 0)
	results := make([]User, 0)

	err := csr.Fetch(&results, 0, false)
	if err != nil {
		t.Errorf("Unable to fetch: %s \n", err.Error())
	} else {
		fmt.Println("======================")
		fmt.Println("Select with FILTER")
		fmt.Println("======================")

		fmt.Printf("Fetch N OK. Result:\n")
		for i := 0; i < len(results); i++ {
			fmt.Printf("%v \n", toolkit.JsonString(results[i]))
		}

	}
}

// func TestSelectAggregate(t *testing.T) {
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("Unable to connect %s \n", e.Error())
// 	}
// 	defer c.Close()

// 	csr, e := c.NewQuery().
// 		Select("nama").
// 		Aggr(dbox.AggrSum, 1, "Total Item").
// 		Aggr(dbox.AggrMax, "amount", "Max Amount").
// 		Aggr(dbox.AggrSum, "amount", "Total Amount").
// 		Aggr(dbox.AggrAvr, "amount", "Average Amount").
// 		From("orders").
// 		Group("nama").
// 		Order("nama").
// 		Skip(2).
// 		Take(1).
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

// 	results := make([]map[string]interface{}, 0)

// 	err := csr.Fetch(&results, 0, false)
// 	if err != nil {
// 		t.Errorf("Unable to fetch: %s \n", err.Error())
// 	} else {
// 		fmt.Println("======================")
// 		fmt.Println("QUERY AGGREGATION")
// 		fmt.Println("======================")
// 		fmt.Printf("Fetch N OK. Result:\n")
// 		for _, val := range results {
// 			fmt.Printf("%v \n",
// 				toolkit.JsonString(val))
// 		}
// 	}
// }

// func TestSelectAggregateUsingCommand(t *testing.T) {
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("Unable to connect %s \n", e.Error())
// 		return
// 	}
// 	defer c.Close()

// 	pipe := []toolkit.M{toolkit.M{}.Set("$group",
// 		toolkit.M{}.Set("_id", "cust_id").
// 			Set("Total Item", toolkit.M{}.Set("$sum", 1)).
// 			Set("Total Amount", toolkit.M{}.Set(dbox.AggrSum, "amount")).
// 			Set("Average Amount", toolkit.M{}.Set(dbox.AggrAvr, "amount")))}
// 	csr, e := c.NewQuery().
// 		Command("pipe", pipe).
// 		From("orders").
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

// 	results := make([]map[string]interface{}, 0)

// 	err := csr.Fetch(&results, 0, false)
// 	if err != nil {
// 		t.Errorf("Unable to fetch: %s \n", err.Error())
// 	} else {
// 		fmt.Println("======================")
// 		fmt.Println("AGGREGATE USING COMMAND")
// 		fmt.Println("======================")
// 		for _, val := range results {
// 			fmt.Printf("Fetch N OK. Result: %v \n",
// 				toolkit.JsonString(val))
// 		}
// 	}
// }

type Marshaler interface {
	MarshalJSON() ([]byte, error)
}

type JSONTime time.Time

func (t JSONTime) MarshalJSON() ([]byte, error) {
	stamp := fmt.Sprintf("\"%s\"", time.Time(t).UTC())
	return []byte(stamp), nil
}

func TestCRUD(t *testing.T) {
	//t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	// ===============================INSERT==============================

	// q := c.NewQuery().SetConfig("multiexec", true).From("tes").Save()

	// dataInsert := User{}
	// dataInsert.Player_id = fmt.Sprintf("30")
	// dataInsert.Nama = fmt.Sprintf("Batistuta")
	// //dataInsert.Tanggal = JSONTime(time.Now())
	// dataInsert.Tanggal = time.Now()
	// dataInsert.Umur = 40

	// e = q.Exec(toolkit.M{
	// 	"data": dataInsert,
	// })
	// if e != nil {
	// 	t.Errorf("Unable to save: %s \n", e.Error())
	// }

	// ===============================INSERT MANY==============================

	// nama := []string{"Toure", "Ivanovic", "Costa", "Chamberlain", "Hart", "Bruyne", "Aguero"}
	// dataInsert := User{}
	// q := c.NewQuery().SetConfig("multiexec", true).From("tes").Save()

	// for i, val := range nama {

	// 	dataInsert.Player_id = strconv.Itoa(i + 16)
	// 	dataInsert.Nama = fmt.Sprintf(val)
	// 	dataInsert.Tanggal = time.Now()
	// 	dataInsert.Umur = i + 26
	// 	e = q.Exec(toolkit.M{
	// 		"data": dataInsert,
	// 	})
	// 	if e != nil {
	// 		t.Errorf("Unable to save: %s \n", e.Error())
	// 	}
	// }

	// ===============================UPDATE==============================

	// data := User{}
	// data.Player_id = "ply030"
	// data.Nama = "Terry"
	// data.Tanggal = time.Now()
	// data.Umur = 35
	// e = c.NewQuery().From("tes").Where(dbox.Eq("player_id", "30")).Update().Exec(toolkit.M{"data": data})
	// if e != nil {
	// 	t.Errorf("Unable to update: %s \n", e.Error())
	// }

	// ===============================UPDATE ALL ID==============================
	// data := UpdateID{}
	// fmt.Println(data)
	// for i := 1; i < 23; i++ {
	// 	data := UpdateID{}
	// 	if i < 10 {
	// 		data.Player_id = "ply00" + strconv.Itoa(i)
	// 	} else {
	// 		data.Player_id = "ply0" + strconv.Itoa(i)
	// 	}

	// 	e = c.NewQuery().From("tes").Where(dbox.Eq("player_id", i)).Update().Exec(toolkit.M{"data": data})
	// 	if e != nil {
	// 		t.Errorf("Unable to update: %s \n", e.Error())
	// 	}

	// }

	// ===============================DELETE==============================
	// e = c.NewQuery().From("tes").Where(dbox.And(dbox.Eq("player_id", "ply030"), dbox.Eq("nama", "Terry"))).Delete().Exec(nil)
	// if e != nil {
	// 	t.Errorf("Unablet to delete table %s\n", e.Error())
	// 	return
	// }

	// ===============================CLEAR ALL TABLE DATA==============================

	// e = c.NewQuery().From("tes").Delete().Exec(nil)
	// if e != nil {
	// 	t.Errorf("Unablet to clear table %s\n", e.Error())
	// 	return
	// }

}
