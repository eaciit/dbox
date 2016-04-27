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

type DataType struct {
	T_Int    int
	T_Float  float64
	T_Bool   bool
	T_String string
	T_Date   time.Time
}

type Player struct {
	Player_id string
	Nama      string
	Umur      int
}

type UpdateID struct {
	Player_id string
}

type Coba struct {
	Id      string
	Nama    string
	Tanggal time.Time
}

type NoID struct {
	Aidi    string
	Nama    string
	Tanggal time.Time
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

func TestProcedure(t *testing.T) {
	t.Skip("")
	c, _ := prepareConnection()
	defer c.Close()

	csr, e := c.NewQuery().Command("procedure", toolkit.M{}.
		Set("name", "staticproc").
		Set("parms", toolkit.M{}.Set("", ""))).
		Cursor(nil)

	// csr, e := c.NewQuery().Command("procedure", toolkit.M{}.
	// 	Set("name", "staticproc")).
	// 	Cursor(nil)

	// csr, e := c.NewQuery().Command("procedure", toolkit.M{}.
	// 	Set("name", "getUmur").
	// 	Set("parms", toolkit.M{}.Set("@nama", "Vidal"))).
	// 	Cursor(nil)

	// csr, e := c.NewQuery().Command("procedure", toolkit.M{}.
	// 	Set("name", "getUmurIn").
	// 	Set("parms", toolkit.M{}.Set("@umur1", 20).Set("@umur2", 23))).
	// 	Cursor(nil)

	// csr, e := c.NewQuery().
	// 	Command("procedure", toolkit.M{}.
	// 	Set("name", "insertdata").
	// 	Set("parms", toolkit.M{}.
	// 	Set("@idIn", "31").
	// 	Set("@namaIn", "Kolarov").
	// 	Set("@umurIn", 40))).
	// 	Cursor(nil)

	// csr, e := c.NewQuery().
	// 	Command("procedure", toolkit.M{}.
	// 	Set("name", "updatedata").
	// 	Set("parms", toolkit.M{}.
	// 	Set("@idIn", "ply031").
	// 	Set("@idCondIn", "31").
	// 	Set("@namaIn", "Batistuta").
	// 	Set("@umurIn", 25))).
	// 	Cursor(nil)

	// csr, e := c.NewQuery().
	// 	Command("procedure", toolkit.M{}.
	// 	Set("name", "deletedata").
	// 	Set("parms", toolkit.M{}.
	// 	Set("@idCondIn", "ply031"))).
	// 	Cursor(nil)

	/*============================ ORACLE =========================*/

	// csr, e := c.NewQuery().Command("procedure", toolkit.M{}.
	// 	Set("name", "staticproc").
	// 	Set("orderparam", []string{""}).
	// 	Set("parms", toolkit.M{}.Set("", ""))).
	// 	Cursor(nil)

	// csr, e := c.NewQuery().Command("procedure", toolkit.M{}.
	// 	Set("name", "staticproc")).
	// 	Cursor(nil)

	// csr, e := c.NewQuery().Command("procedure", toolkit.M{}.
	// 	Set("name", "getUmur").
	// 	Set("orderparam", []string{"@nama", "@@o_umur"}).
	// 	Set("parms", toolkit.M{}.Set("@nama", "Vidal").
	// 	Set("@@o_umur", "number"))).
	// 	Cursor(nil)

	// csr, e := c.NewQuery().Command("procedure", toolkit.M{}.
	// 	Set("name", "getUmurIn").
	// 	Set("orderparam", []string{"@p_umur1", "@p_umur2", "@@o_nama", "@@o_umur"}).
	// 	Set("parms", toolkit.M{}.Set("@p_umur1", "20").Set("@p_umur2", "20").
	// 	Set("@@o_nama", "varchar2(255)").Set("@@o_umur", "number"))).
	// 	Cursor(nil)

	// csr, e := c.NewQuery().
	// 	Command("procedure", toolkit.M{}.
	// 	Set("name", "insertdata").
	// 	Set("orderparam", []string{"@idIn", "@nameIn", "@umurIn"}).
	// 	Set("parms", toolkit.M{}.
	// 	Set("@idIn", "30").
	// 	Set("@nameIn", "Costacurta").
	// 	Set("@umurIn", 40))).
	// 	Cursor(nil)

	// csr, e := c.NewQuery().
	// 	Command("procedure", toolkit.M{}.
	// 	Set("name", "updatedata").
	// 	Set("orderparam", []string{"@idIn", "@idCondIn", "@nameIn", "@umurIn"}).
	// 	Set("parms", toolkit.M{}.
	// 	Set("@idIn", "ply030").
	// 	Set("@idCondIn", "30").
	// 	Set("@nameIn", "Payet").
	// 	Set("@umurIn", 25))).
	// 	Cursor(nil)

	// csr, e := c.NewQuery().
	// 	Command("procedure", toolkit.M{}.
	// 	Set("name", "deletedata").
	// 	Set("orderparam", []string{"@idCondIn"}).
	// 	Set("parms", toolkit.M{}.
	// 	Set("@idCondIn", "ply030"))).
	// 	Cursor(nil)

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

func TestUpdate(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	data := User{}
	data.Player_id = "1"
	data.Nama = "Bourne"
	data.Tanggal = time.Now()
	data.Umur = 23

	e = c.NewQuery().From("tes").Where(dbox.Eq("player_id", "ply001")).Update().Exec(toolkit.M{"data": data})
	if e != nil {
		t.Errorf("Unable to update: %s \n", e.Error())
	}
}

func TestSelectFilter(t *testing.T) {
	// t.Skip("")
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	layoutFormat := "2006-01-02 15:04:05"
	dateValue1 := "2016-01-26 3:26:40"
	dateValue2 := "2016-01-26 3:26:39"
	var tanggal1 time.Time
	var tanggal2 time.Time
	tanggal1, _ = time.Parse(layoutFormat, dateValue1)
	tanggal2, _ = time.Parse(layoutFormat, dateValue2)
	_ = tanggal1
	_ = tanggal2

	csr, e := c.NewQuery().
		// Select("player_id", "nama", "tanggal", "umur").
		// From("tes").
		Select("t_int", "t_float", "t_bool", "t_string", "t_date").
		From("tipedata").
		// Where(dbox.Eq("nama", "Bourne")).
		// Where(dbox.Ne("nama", "Bourne")).
		// Where(dbox.Gt("umur", 25)).
		// Where(dbox.Gte("umur", 25)).
		// Where(dbox.Lt("umur", 25)).
		// Where(dbox.Lte("tanggal", tanggal1)).
		// Where(dbox.Lte("umur", 25)).
		// Where(dbox.In("nama", "vidal", "bourne")).
		// Where(dbox.In("umur", 25, 30)).
		// Where(dbox.Nin("umur", 25, 30)).
		// Where(dbox.In("tanggal", tanggal1, tanggal2)).
		// Where(dbox.And(dbox.Gt("umur", 25), dbox.Eq("nama", "Roy"))).
		// Where(dbox.Or(dbox.Lte("umur", 25), dbox.Eq("nama", "Roy"))).
		// Where(dbox.Contains("nama", "ar", "ov")).
		// Where(dbox.Startwith("nama", "Ba")).
		// Where(dbox.Endwith("nama", "ta")).
		// Order("nama").
		// Skip(2).
		// Take(1).
		Cursor(nil)
	// Where(dbox.Eq("nama", "@nama")).
	// Cursor(toolkit.M{}.Set("nama", "clyne"))
	// Where(dbox.Eq("umur", "@age")).
	// Cursor(toolkit.M{}.Set("age", 25))
	// Where(dbox.Ne("umur", "@age")).
	// Cursor(toolkit.M{}.Set("age", 25))
	// Where(dbox.Gt("tanggal", "@date")).
	// Cursor(toolkit.M{}.Set("date", tanggal1))
	// Where(dbox.Gt("umur", "@age")).
	// Cursor(toolkit.M{}.Set("age", 25))
	// Where(dbox.Gte("umur", "@age")).
	// Cursor(toolkit.M{}.Set("age", 25))
	// Where(dbox.Lt("umur", "@age")).
	// Cursor(toolkit.M{}.Set("age", 25))
	// Where(dbox.Lte("umur", "@age")).
	// Cursor(toolkit.M{}.Set("age", 25))
	// Where(dbox.In("nama", "@nama1", "@nama2")).
	// Cursor(toolkit.M{}.Set("nama1", "clyne").Set("nama2", "Kane"))
	// Where(dbox.Nin("nama", "@nama1", "@nama2")).
	// Cursor(toolkit.M{}.Set("nama1", "clyne").Set("nama2", "Kane"))
	// Where(dbox.And(dbox.Gt("umur", "@age"), dbox.Eq("nama", "@nama"))).
	// Cursor(toolkit.M{}.Set("age", 25).Set("nama", "Kane"))
	// Where(dbox.Or(dbox.Lte("umur", "@age"), dbox.Eq("nama", "@nama"))).
	// Cursor(toolkit.M{}.Set("age", 23).Set("nama", "Kane"))
	// Where(dbox.And(dbox.Or(dbox.Eq("nama", "@nama1"), dbox.Eq("nama", "@nama2"),
	// dbox.Eq("nama", "@nama3")), dbox.Lt("umur", "@age"))).
	// Cursor(toolkit.M{}.Set("nama1", "Kane").Set("nama2", "Roy").
	// Set("nama3", "Oscar").Set("age", 30))
	// Where(dbox.Contains("nama", "@name1")).
	// Cursor(toolkit.M{}.Set("name1", "Os"))
	// Where(dbox.Startwith("nama", "@nama1")).
	// Cursor(toolkit.M{}.Set("nama1", "Ba"))
	// Where(dbox.Endwith("nama", "@nama1")).
	// Cursor(toolkit.M{}.Set("nama1", "ta"))

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
	// results := make([]User, 0)
	results := make([]DataType, 0)

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

func TestSelectAggregate(t *testing.T) {
	t.Skip("")
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Select("nama").
		Aggr(dbox.AggrSum, 1, "Total Item").
		Aggr(dbox.AggrMax, "amount", "Max Amount").
		Aggr(dbox.AggrMin, "amount", "Min Amount").
		Aggr(dbox.AggrSum, "amount", "Total Amount").
		Aggr(dbox.AggrAvr, "amount", "Average Amount").
		From("orders").
		Group("nama").
		Order("nama").
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
		fmt.Printf("Fetch N OK. Result:\n")
		for _, val := range results {
			fmt.Printf("%v \n",
				toolkit.JsonString(val))
		}
	}
}

func TestCRUD(t *testing.T) {
	t.Skip("")
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	/* ===============================INSERT============================== */
	// q := c.NewQuery().From("tes").Insert()
	// dataInsert := User{}
	// dataInsert.Player_id = fmt.Sprintf("30")
	// dataInsert.Nama = fmt.Sprintf("Batistuta")
	// //dataInsert.Tanggal = JSONTime(time.Now())
	// dataInsert.Tanggal = time.Now()
	// dataInsert.Umur = 40

	// e = q.Exec(toolkit.M{"data": dataInsert})
	// if e != nil {
	// 	t.Errorf("Unable to save: %s \n", e.Error())
	// }

	/* ===============================INSERT MANY============================== */

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

	/* ===============================SAVE DATA============================== */
	// q := c.NewQuery().SetConfig("multiexec", true).From("coba").Save()
	// dataInsert := Coba{}
	// dataInsert.Id = fmt.Sprintf("1")
	// dataInsert.Nama = fmt.Sprintf("multi true, with data contains ID, update")
	// dataInsert.Tanggal = time.Now()

	// q := c.NewQuery().SetConfig("multiexec", true).From("noid").Save()
	// dataInsert := NoID{}
	// dataInsert.Aidi = fmt.Sprintf("10")
	// dataInsert.Nama = fmt.Sprintf("multi false, with data contains no ID")
	// dataInsert.Tanggal = time.Now()

	// e = q.Exec(toolkit.M{"data": dataInsert})
	// // e = q.Exec(nil)
	// if e != nil {
	// 	t.Errorf("Unable to save data : %s \n", e.Error())
	// }

	/* ===============================UPDATE============================== */

	// data := User{}
	// data.Player_id = "ply030"
	// data.Nama = "Terry"
	// data.Tanggal = time.Now()
	// data.Umur = 35

	// e = c.NewQuery().From("coba").Where(dbox.Eq("player_id", "30")).Update().Exec(toolkit.M{"data": data})
	// if e != nil {
	// 	t.Errorf("Unable to update: %s \n", e.Error())
	// }

	// data := NoID{}
	// data.Aidi = fmt.Sprintf("10")
	// data.Nama = fmt.Sprintf("multi true, with conditons, with data contains no ID")
	// data.Tanggal = time.Now()

	// data := Coba{}
	// data.Id = fmt.Sprintf("1")
	// data.Nama = fmt.Sprintf("multi true, with data contains ID, with condition, update")
	// data.Tanggal = time.Now()

	// e = c.NewQuery().SetConfig("multiexec", true).From("noid").Update().
	// 	Where(dbox.Eq("aidi", "10")).
	// 	Exec(toolkit.M{"data": data})
	// // Exec(nil)
	// if e != nil {
	// 	t.Errorf("Unable to update: %s \n", e.Error())
	// }

	/* ===============================UPDATE ALL ID============================== */
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

	/* ===============================DELETE==============================*/
	// e = c.NewQuery().From("tes").Where(dbox.And(dbox.Eq("player_id", "ply030"), dbox.Eq("nama", "Terry"))).Delete().Exec(nil)
	// e = c.NewQuery().From("noid").SetConfig("multiexec", false).Delete().
	// 	// Where(dbox.Eq("aidi", "10")).
	// 	Exec(nil)
	// if e != nil {
	// 	t.Errorf("Unable to delete table %s\n", e.Error())
	// 	return
	// }

	/* ===============================CLEAR ALL TABLE DATA==============================*/

	// e = c.NewQuery().From("tes").Delete().Exec(nil)
	// if e != nil {
	// 	t.Errorf("Unablet to clear table %s\n", e.Error())
	// 	return
	// }

}
