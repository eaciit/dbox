package mysql

import (
	"fmt"
	"github.com/eaciit/dbox"
	"github.com/eaciit/toolkit"
	//"reflect"
	// "strconv"
	"testing"
	"time"
)

type User struct {
	Id      string
	Name    string
	Tanggal time.Time
	Umur    int
}

type DataType struct {
	T_Int    int
	T_Float  float64
	T_Bool   bool
	T_String string
	T_Date   time.Time
}

type Player struct {
	Id   string
	Name string
	Umur int
}

type UpdateID struct {
	Id string
}

type Coba struct {
	Id   string
	Name string
}

type NoID struct {
	Aidi string
	Name string
}

func prepareConnection() (dbox.IConnection, error) {
	config := toolkit.M{}
	config.Set("dateformat", "2006-01-02 15:04:05")
	ci := &dbox.ConnectionInfo{"localhost:3306", "test", "root", "", config}
	c, e := dbox.NewConnection("mysql", ci)
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
		return
	} else {
		defer c.Close()
	}
}

func TestFetch(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()

	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	//csr, e := c.NewQuery().Select().From("tes").Where(dbox.Eq("id", "3")).Cursor(nil)
	csr, e := c.NewQuery().
		Select("id", "name").
		From("tes").
		Take(10).
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
		t.Errorf("Unable to fetch all: %s \n", err.Error())
	} else {
		toolkit.Println("=========================")
		toolkit.Println("Select with NO filter")
		toolkit.Println("=========================")
		toolkit.Println("Fetch N OK. Result:")

		for _, val := range results {
			fmt.Printf("%v \n",
				toolkit.JsonString(val))
		}
	}

	e = csr.ResetFetch()
	if e != nil {
		t.Errorf("Unable to reset fetch: %s \n", e.Error())
	}

	err = csr.Fetch(&results, 1, false)
	if err != nil {
		t.Errorf("Unable to fetch all: %s \n", err.Error())
	} else {
		toolkit.Println("=========================")
		toolkit.Println("Select Fetch")
		toolkit.Println("=========================")
		toolkit.Println("Fetch N OK. Result:")

		for _, val := range results {
			fmt.Printf("%v \n",
				toolkit.JsonString(val))
		}
	}

	err = csr.Fetch(&results, 2, false)
	if err != nil {
		t.Errorf("Unable to fetch all: %s \n", err.Error())
	} else {
		toolkit.Println("=========================")
		toolkit.Println("Select Fetch")
		toolkit.Println("=========================")
		toolkit.Println("Fetch N OK. Result:")

		for _, val := range results {
			fmt.Printf("%v \n",
				toolkit.JsonString(val))
		}
	}

	err = csr.Fetch(&results, 0, false)
	if err != nil {
		t.Errorf("Unable to fetch all: %s \n", err.Error())
	} else {
		toolkit.Println("=========================")
		toolkit.Println("Select Fetch")
		toolkit.Println("=========================")
		toolkit.Println("Fetch N OK. Result:")

		for _, val := range results {
			fmt.Printf("%v \n",
				toolkit.JsonString(val))
		}
	}

	err = csr.Fetch(&results, 2, false)
	if err != nil {
		t.Errorf("Unable to fetch all: %s \n", err.Error())
	} else {
		toolkit.Println("=========================")
		toolkit.Println("Select Fetch")
		toolkit.Println("=========================")
		toolkit.Println("Fetch N OK. Result:")

		for _, val := range results {
			fmt.Printf("%v \n",
				toolkit.JsonString(val))
		}
	}
}

// func TestFilter(t *testing.T) {
// 	fb := dbox.NewFilterBuilder(new(FilterBuilder))
// 	fb.AddFilter(dbox.And(
// 		dbox.Eq("_id", "33"),dbox.Eq("_id", "34"),dbox.Eq("_id", "35")))
// 	// fb.AddFilter(dbox.Eq("_id", "33"), dbox.Eq("_id", "35"))
// 	b, e := fb.Build()
// 	if e != nil {
// 		t.Errorf("Error %s", e.Error())
// 	} else {
// 		fmt.Printf("Result:\n%v\n", toolkit.JsonString(b))
// 	}

// }

// func TestSelect(t *testing.T) {
// 	c, e := prepareConnection()

// 	if e != nil {
// 		t.Errorf("Unable to connect %s \n", e.Error())
// 	}
// 	defer c.Close()

// 	//csr, e := c.NewQuery().Select().From("tes").Where(dbox.Eq("id", "3")).Cursor(nil)
// 	csr, e := c.NewQuery().Select("id", "name").From("tes").Cursor(nil)

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
	t.Skip()
	c, _ := prepareConnection()
	defer c.Close()

	//csr, e := c.NewQuery().Command("procedure", toolkit.M{}.Set("name", "getUmur").Set("parms", toolkit.M{}.Set("@name", "Vidal"))).Cursor(nil)
	//====================CALL MY SQL STORED PROCEDURE====================
	//Without output
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

	csr, e := c.NewQuery().
		Command("procedure", toolkit.M{}.
		Set("name", "getNamaUmur").
		Set("orderparam", []string{""}).
		Set("parms", toolkit.M{}.
		Set("", ""))).
		Cursor(nil)

	// With output
	// csr, e := c.NewQuery().
	// 	Command("procedure", toolkit.M{}.
	// 	Set("name", "twooutput").
	// 	Set("orderparam", []string{"@@umurOut", "@nameIn", "@umurIn", "@@nameOut"}).
	// 	Set("parms", toolkit.M{}.
	// 	Set("@@umurOut", "int").
	// 	Set("@nameIn", "Kane").
	// 	Set("@umurIn", 29).
	// 	Set("@@nameOut", "varchar(255)"))).
	// 	Cursor(nil)

	// csr, e := c.NewQuery().
	// 	Command("procedure", toolkit.M{}.
	// 	Set("name", "getUmurIn").
	// 	Set("orderparam", []string{"@umur1", "@umur2", "@@o_umur"}).
	// 	Set("parms", toolkit.M{}.
	// 	Set("@umur1", 20).
	// 	Set("@umur2", 23).
	// 	Set("@@o_umur", "int"))).
	// 	Cursor(nil)

	// in out example
	// csr, e := c.NewQuery().
	// 	Command("procedure", toolkit.M{}.
	// 	Set("name", "inoutproc").
	// 	Set("orderparam", []string{"@@umur", "@nameIn", "@@nameOut"}).
	// 	Set("parms", toolkit.M{}.
	// 	Set("@@umurOut", "int").
	// 	Set("@nameIn", "Kane").
	// 	Set("@@nameOut", "varchar(255)"))).
	// 	Cursor(nil)

	// csr, e := c.NewQuery().
	// 	Command("procedure", toolkit.M{}.
	// 	Set("name", "getUmurAjah").
	// 	Set("orderparam", []string{"@nama"}).
	// 	Set("parms", toolkit.M{}.
	// 	Set("@nama", "Kane"))).
	// 	Cursor(nil)
	//====================CALL ORACLE STORED PROCEDURE====================
	// csr, e := c.NewQuery().
	// 	Command("procedure", toolkit.M{}.
	// 	Set("name", "getUmurIn").
	// 	Set("parms", toolkit.M{}.
	// 	Set("@p_umur1", "20").
	// 	Set("@p_umur2", "23").
	// 	Set("@@o_nama", "varchar2").
	// 	Set("@@o_umur", "number"))).
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

func TestSelectFilter(t *testing.T) {
	// t.Skip()
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
	_ = tanggal1
	_ = tanggal2
	csr, e := c.NewQuery().
		// Select("id", "name", "tanggal", "umur").
		// From("tes").
		Select("t_int", "t_float", "t_bool", "t_string", "t_date").
		From("tipedata").
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
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Select("nama").
		Aggr(dbox.AggrSum, 1, "Total Item").
		Aggr(dbox.AggrMax, "amount", "Max Amount").
		Aggr(dbox.AggrSum, "amount", "Total Amount").
		Aggr(dbox.AggrAvr, "amount", "Average Amount").
		From("orders").
		Group("nama").
		Order("nama").
		Skip(2).
		Take(1).
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
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	// ===============================INSERT==============================
	// q := c.NewQuery().From("tes").Insert()
	// dataInsert := User{}
	// dataInsert.Id = fmt.Sprintf("40")
	// dataInsert.Name = fmt.Sprintf("New Player")
	// dataInsert.Tanggal = time.Now()
	// dataInsert.Umur = 40

	// e = q.Exec(toolkit.M{"data": dataInsert})
	// if e != nil {
	// 	t.Errorf("Unable to insert data : %s \n", e.Error())
	// }

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
	// dataInsert.Id = fmt.Sprintf("3")
	// dataInsert.Name = fmt.Sprintf("update data")

	// q := c.NewQuery().SetConfig("multiexec", false).From("NoID").Save()
	// dataInsert := NoID{}
	// dataInsert.Aidi = fmt.Sprintf("40")
	// dataInsert.Name = fmt.Sprintf("no update")

	// e = q.Exec(toolkit.M{"data": dataInsert})
	// if e != nil {
	// 	t.Errorf("Unable to insert data : %s \n", e.Error())
	// }

	/* ===============================UPDATE============================== */

	// data := User{}
	// data.Id = "40"
	// data.Name = "player40"
	// data.Tanggal = time.Now()
	// data.Umur = 24
	// e = c.NewQuery().From("tes").Where(dbox.Eq("id", "30")).Update().Exec(toolkit.M{"data": data})
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
	// e = c.NewQuery().From("tes").Where(dbox.And(dbox.Eq("id", "2"), dbox.Eq("name", "Thuram"))).Delete().Exec(nil)
	// if e != nil {
	// 	t.Errorf("Unable to delete table %s\n", e.Error())
	// 	return
	// }

	// data := User{}
	// data.Id = "40"

	// e = c.NewQuery().From("tes").Delete().Exec(toolkit.M{"data": data})
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

func TestViewAllTables(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("unnable  to connect %s \n", e.Error())
	}
	defer c.Close()

	csr := c.ObjectNames(dbox.ObjTypeTable)

	for i := 0; i < len(csr); i++ {
		fmt.Printf("show name table %v \n", toolkit.JsonString(csr[i]))
	}

}

func TestViewProcedureName(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("unnable  to connect %s \n", e.Error())
	}
	defer c.Close()

	proc := c.ObjectNames(dbox.ObjTypeProcedure)

	for i := 0; i < len(proc); i++ {
		fmt.Printf("show name procdure %v \n", toolkit.JsonString(proc[i]))
	}

}

func TestViewName(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("unnable  to connect %s \n", e.Error())
	}
	defer c.Close()

	view := c.ObjectNames(dbox.ObjTypeView)

	for i := 0; i < len(view); i++ {
		fmt.Printf("show name view %v \n", toolkit.JsonString(view[i]))
	}

}

func TestAllObj(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("unnable  to connect %s \n", e.Error())
	}
	defer c.Close()

	all := c.ObjectNames(dbox.ObjTypeAll)

	fmt.Println(all)
	for i := 0; i < len(all); i++ {
		fmt.Printf("show objects %v \n", toolkit.JsonString(all[i]))
	}

}
