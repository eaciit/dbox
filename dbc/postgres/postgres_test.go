package postgres

import (
	"fmt"
	"github.com/eaciit/dbox"
	"github.com/eaciit/toolkit"
	"testing"
	"time"
	//"strconv"
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

type Coba struct {
	Id      string
	Name    string
	Tanggal time.Time
}

type NoId struct {
	Aidi    string
	Nama    string
	Tanggal time.Time
}

func prepareConnection() (dbox.IConnection, error) {
	// ci := &dbox.ConnectionInfo{"localhost:5432", "test", "postgres", "", nil}
	ci := &dbox.ConnectionInfo{"localhost", "test", "postgres", "postgres", nil}
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
	} else {
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
	// t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	csr, e := c.NewQuery().
		// Select("player_id", "nama", "tanggal", "umur").
		// From("tes").
		Select("t_int", "t_float", "t_bool", "t_string", "t_date").
		From("tipedata").
		// Where(dbox.Eq("nama", "buku")).
		Cursor(nil)
	// Where(dbox.Lte("price", "@price")).
	// Cursor(toolkit.M{}.Set("@price", 100000))
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

// func TestCRUD(t *testing.T) {
// t.Skip()
// c, e := prepareConnection()
// if e != nil {
// 	t.Errorf("Unable to connect %s \n", e.Error())
// 	return
// }
// //================================================== for delete ==================================================

// defer c.Close()
// //e = c.NewQuery().From("tes").Delete().Exec(nil)
// // if e != nil {
// // 	t.Errorf("Unablet to clear table %s\n", e.Error())
// // 	return
// // }
// e = c.NewQuery().From("tes").Where(dbox.Eq("id", "123")).Delete().Exec(nil)
// //e = c.NewQuery().From("tes").Where(dbox.Eq("id", "1111"), dbox.Eq("name", "budi")).Delete().Exec(nil)
// if e != nil {
// 	t.Errorf("Unablet to delete table %s\n", e.Error())
// 	return
// }

// //================================================= for save =======================================================

// defer c.Close()
// q := c.NewQuery().SetConfig("multiexec", true).From("tes").Save()
// type user struct {
// 	Id   string
// 	Name string
// 	Date time.Time
// }

// // 	// 	//go func(q dbox.IQuery, i int) {
// data := user{}
// data.Id = "5555"
// data.Name = "sasa"
// data.Date = time.Now().UTC()

// // 		fmt.Println("data >>",data)
// e = q.Exec(toolkit.M{
// 	"data": data,
// })
// e = q.Exec(toolkit.M{
// 	"data": data,
// })
// if e != nil {
// 	t.Errorf("Unable to save: %s \n", e.Error())
// }
// 	defer c.Close()
// }

// q.Close()
// 	data.Id = "555"
// 	data.Name = "aji"
// 	e = c.NewQuery().From("tes").Where(dbox.Eq("id", "555")).Update().Exec(toolkit.M{"data": data})
// 	if e != nil {
// 		t.Errorf("Unable to update: %s \n", e.Error())
// 	}

// }

/*=================================================test procedures=====================================================*/

// func TestProcedures(t *testing.T) {
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("unable to connect %s \n", e.Error())
// 		return
// 	}
// 	defer c.Close()

///*================================================function using parameters===========================================*/

// csr, e := c.NewQuery().
// 	Command("procedure", toolkit.M{}.
// 	Set("name", "add_name").
// 	Set("orderparam", []string{"@id", "@nama"}).
// 	Set("parms", toolkit.M{}.
// 	Set("@id", "2225").
// 	Set("@nama", "didu"))).
// 	Cursor(nil)

///*=================================================function no parameters=============================================*/

// 	csr, e := c.NewQuery().
// 		Command("procedure", toolkit.M{}.
// 		Set("name", "show_again").
// 		Set("orderparam", []string{""}).
// 		Set("parms", toolkit.M{}.
// 		Set("", ""))).
// 		Cursor(nil)

// 	if e != nil {
// 		t.Errorf("cursor pre error : %s \n", e.Error())
// 		return
// 	}
// 	if csr == nil {
// 		t.Errorf("cursor not initialized")
// 		return
// 	}
// 	defer csr.Close()

// 	result := make([]map[string]interface{}, 0)
// 	fmt.Println("+++++++++++++", result)
// 	err := csr.Fetch(&result, 0, false)
// 	if err != nil {
// 		t.Errorf("unnable to fetch : %s \n", err.Error())
// 	} else {
// 		fmt.Println("======================")
// 		fmt.Println("Select to call function")
// 		fmt.Println("======================")

// 		fmt.Printf("Fetch N OK. Result:\n")
// 		for i := 0; i < len(result); i++ {
// 			fmt.Printf("%v \n", toolkit.JsonString(result[i]))
// 		}
// 	}
// }

// // func TestSelect(t *testing.T) {
// // 	c, e := prepareConnection()
// // 	if e != nil {
// // 		t.Errorf("unable to connect %s \n", e.Error())
// // 		return
// // 	}

// 	defer c.Close()

// 	//csr, e := c.NewQuery().Select("id", "name", "tanggal", "umur").From("tes").Cursor(nil)
// 	//csr, e := c.NewQuery().Select("id", "name", "tanggal", "umur").From("tes").Where(dbox.Eq("name", "Bourne")).Cursor(nil)
// 	//csr, e := c.NewQuery().Select("id", "name", "tanggal", "umur").From("tes").Where(dbox.Ne("name", "Bourne")).Cursor(nil)
// 	//csr, e := c.NewQuery().Select("id", "name", "tanggal", "umur").From("tes").Where(dbox.Gt("umur", "25")).Cursor(nil)
// 	//csr, e := c.NewQuery().Select("id", "name", "tanggal", "umur").From("tes").Where(dbox.Gte("umur", "25")).Cursor(nil)
// 	csr, e := c.NewQuery().Select("id", "name", "tanggal", "umur").From("tes").Where(dbox.Lte("tanggal", "2016-01-18 20:41:48")).Cursor(nil)
// 	//csr, e := c.NewQuery().Select("id", "name", "tanggal", "umur").From("tes").Where(dbox.Lte("umur", "25")).Cursor(nil)
// 	//csr, e := c.NewQuery().Select("id", "name", "tanggal", "umur").From("tes").Where(dbox.In("name", "vidal", "Bourne")).Cursor(nil)
// 	//csr, e := c.NewQuery().Select("id", "name", "tanggal", "umur").From("tes").Where(dbox.In("umur", "25", "30")).Cursor(nil)
// 	//csr, e := c.NewQuery().Select("id", "name", "tanggal", "umur").From("tes").Where(dbox.Nin("umur", "25", "30")).Cursor(nil)
// 	//csr, e := c.NewQuery().Select("id", "name", "tanggal", "umur").From("tes").Where(dbox.In("tanggal", "2016-01-12 14:35:54", "2016-01-12 14:36:15")).Cursor(nil)
// 	//csr, e := c.NewQuery().Select("id", "name", "tanggal", "umur").From("tes").Where(dbox.Gt("umur", "25"), dbox.Eq("name", "Roy")).Cursor(nil)

// 	if e != nil {
// 		t.Errorf("cursor pre error : %s \n", e.Error())
// 	}

// 	if csr == nil {
// 		t.Errorf("cursor not initialized")
// 		return
// 	}

// 	result := make([]map[string]interface{}, 0)
// 	//fmt.Println("===========", result)
// 	err := csr.Fetch(&result, 0, false)
// 	if err != nil {
// 		t.Errorf("unnable to fetch : %s \n", err.Error())
// 	} else {
// 		fmt.Println("======================")
// 		fmt.Println("Select data tes")
// 		fmt.Println("======================")

// 		fmt.Printf("Fetch N OK. Result:\n")
// 		for i := 0; i < len(result); i++ {
// 			fmt.Printf("%v \n", toolkit.JsonString(result[i]))
// 		}
// 	}
// }

// func TestContainer(t *testing.T) {
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("unnable  to connect %s \n", e.Error())
// 	}
// 	defer c.Close()

// 	//csr, e := c.NewQuery().Select("id", "name", "umur").From("tes").Where(dbox.Contains("name", "ar")).Cursor(nil)
// 	//csr, e := c.NewQuery().Select("id", "name", "umur").From("tes").Where(dbox.Contains("name", "ar", "ov")).Cursor(nil)
// 	csr, e := c.NewQuery().Select("id", "name", "umur").From("tes").Where(dbox.Or(dbox.Contains("name", "oy"), dbox.Contains("name", "Os"))).Cursor(nil)

// 	if e != nil {
// 		t.Errorf("cursor pre error : %s \n", e.Error())
// 		return
// 	}

// 	if csr == nil {
// 		t.Errorf("cursor not initialized")
// 	}

// 	results := make([]map[string]interface{}, 0)

// 	err := csr.Fetch(&results, 0, false)
// 	if err != nil {
// 		t.Errorf("unnable to fetch: %s \n", err.Error())
// 	} else {
// 		fmt.Println("===========================")
// 		fmt.Println("contain data")
// 		fmt.Println("===========================")

// 		fmt.Println("fetch N Ok. Result :\n")

// 		for i := 0; i < len(results); i++ {
// 			fmt.Printf("%v \n", toolkit.JsonString(results[i]))
// 		}
// 	}

// }

//============================================ Startwith or Endwith =============================

// func TestStartorEndWith(t *testing.T) {
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("unnable  to connect %s \n", e.Error())
// 	}
// 	defer c.Close()

// 	csr, e := c.NewQuery().Select("id", "name", "umur").From("tes").Where(dbox.Startwith("name", "Bo")).Cursor(nil)
// 	//csr, e := c.NewQuery().Select("id", "name", "umur").From("tes").Where(dbox.Endwith("name", "ar")).Cursor(nil)

// 	if e != nil {
// 		t.Errorf("cursor pre error : %s \n", e.Error())
// 		return
// 	}

// 	if csr == nil {
// 		t.Errorf("cursor not initialized")
// 	}

// 	results := make([]map[string]interface{}, 0)

// 	err := csr.Fetch(&results, 0, false)
// 	if err != nil {
// 		t.Errorf("unnable to fetch: %s \n", err.Error())
// 	} else {
// 		fmt.Println("===========================")
// 		fmt.Println("contain data")
// 		fmt.Println("===========================")

// 		fmt.Println("fetch N Ok. Result :\n")

// 		for i := 0; i < len(results); i++ {
// 			fmt.Printf("%v \n", toolkit.JsonString(results[i]))
// 		}
// 	}

// }

// func TestViewAllTables(t *testing.T) {
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("unnable  to connect %s \n", e.Error())
// 	}
// 	defer c.Close()

// 	csr := c.ObjectNames(dbox.ObjTypeTable)

// 	for i := 0; i < len(csr); i++ {
// 		fmt.Printf("show name table %v \n", toolkit.JsonString(csr[i]))
// 	}

// }

// func TestViewProcedureName(t *testing.T) {
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("unnable  to connect %s \n", e.Error())
// 	}
// 	defer c.Close()

// 	proc := c.ObjectNames(dbox.ObjTypeProcedure)

// 	for i := 0; i < len(proc); i++ {
// 		fmt.Printf("show name procdure %v \n", toolkit.JsonString(proc[i]))
// 	}

// }

// func TestViewName(t *testing.T) {
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("unnable  to connect %s \n", e.Error())
// 	}
// 	defer c.Close()

// 	view := c.ObjectNames(dbox.ObjTypeView)

// 	for i := 0; i < len(view); i++ {
// 		fmt.Printf("show name view %v \n", toolkit.JsonString(view[i]))
// 	}

// }

// func TestAllObj(t *testing.T) {
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("unnable  to connect %s \n", e.Error())
// 	}
// 	defer c.Close()

// 	all := c.ObjectNames(dbox.ObjTypeAll)

// 	fmt.Println(all)
// 	for i := 0; i < len(all); i++ {
// 		fmt.Printf("show objects %v \n", toolkit.JsonString(all[i]))
// 	}

// }

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
// 		Aggr(dbox.AggrMed, "amount", "Median Amount").
// 		Aggr(dbox.AggrAvr, "amount", "Average Amount").
// 		From("orders").
// 		Group("nama").
// 		Order("nama").
// 		Skip(2).
// 		Take(1).
// 		Cursor(nil)

// csr, e := c.NewQuery().
// 	Select("nama").
// 	Aggr(dbox.AggrSum, 1, "Total Item").
// 	Aggr(dbox.AggrMin, "amount", "Min Amount").
// 	Aggr(dbox.AggrSum, "amount", "Total Amount").
// 	Aggr(dbox.AggrAvr, "amount", "Average Amount").
// 	From("orders").
// 	Group("nama").
// 	Order("nama").
// 	Skip(2).
// 	Take(1).
// 	Cursor(nil)
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
// 		for _, val := range results {
// 			fmt.Printf("Fetch N OK. Result: %v \n",
// 				toolkit.JsonString(val))
// 		}
// 	}
// }

func TestCRUD(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	/*===============================INSERT==============================*/
	// q := c.NewQuery().SetConfig("multiexec", true).From("coba").Insert()
	// dataInsert := coba{}
	// dataInsert.Id = fmt.Sprintf("1")
	// dataInsert.Name = fmt.Sprintf("multi, with data contains ID, update")
	// //dataInsert.Tanggal = JSONTime(time.Now())
	// //dataInsert.Tanggal = time.Now()
	// //dataInsert.Umur = 45

	// //e = q.Exec(nil)
	// e = q.Exec(toolkit.M{"data": dataInsert})
	// if e != nil {
	// 	t.Errorf("Unable to insert data : %s \n", e.Error())
	// }

	// ===============================INSERT MANY==============================
	// q := c.NewQuery().SetConfig("multiexec", true).From("coba").Insert()
	// nama := []string{"Barkley", "multi, with data contains ID, update", "multi, with data contains ID, update", "multi, with data contains ID, update"}
	// dataInsert := coba{}

	// for i, val := range nama {

	// 	dataInsert.Id = strconv.Itoa(i + 1)
	// 	dataInsert.Name = fmt.Sprintf(val)
	// 	// dataInsert.Tanggal = time.Now()
	// 	// dataInsert.Umur = i + 20
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
	// dataInsert.Name = fmt.Sprintf("multi, with data contains ID, update")
	// dataInsert.Tanggal = time.Now()

	// q := c.NewQuery().SetConfig("multiexec", false).From("coba").Save()
	// dataInsert := Coba{}
	// dataInsert.Id = fmt.Sprintf("1")
	// dataInsert.Name = fmt.Sprintf("multi, with nnnnnnnnnnnnn, update")
	// dataInsert.Tanggal = time.Now()

	// q := c.NewQuery().SetConfig("multiexec", true).From("noid").Save()
	// nama := []string{"Barkley", "aaaaaa", "bbbbbbb", "cccccc"}
	// for _, val := range nama {
	// 	dataWithNoID := NoId{}
	// 	dataWithNoID.Aidi = fmt.Sprintf("1")
	// 	dataWithNoID.Nama = fmt.Sprintf(val)
	// 	dataWithNoID.Tanggal = time.Now()

	// 	e = q.Exec(toolkit.M{"data": dataWithNoID})
	// 	if e != nil {
	// 		t.Errorf("Unable to insert data : %s \n", e.Error())
	// 	}
	// }

	// 	.SetConfig("multiexec", false)
	// .Exec(toolkit.M{"data": data})
	// (insert new data)

	// q := c.NewQuery().SetConfig("multiexec", false).From("NoID").Save()
	// dataInsert := NoID{}
	// dataInsert.Aidi = fmt.Sprintf("30")
	// dataInsert.Name = fmt.Sprintf("no multi, with data contains no ID")

	// e = q.Exec(toolkit.M{"data": dataWithNoID})
	// if e != nil {
	// 	t.Errorf("Unable to insert data : %s \n", e.Error())
	// }

	/* ===============================UPDATE============================== */

	// data := Coba{}
	// data.Id = "1"
	// data.Name = "Doni"
	// e = c.NewQuery().SetConfig("multiexec", true).From("coba").Update().Exec(nil)
	// if e != nil {
	// 	t.Errorf("Unable to update: %s \n", e.Error())
	// }

	data := Coba{}
	data.Id = "1"
	data.Name = "dadadadadadadaaa"
	e = c.NewQuery().SetConfig("multiexec", true).From("coba").Update().Exec(toolkit.M{"data": data})
	if e != nil {
		t.Errorf("Unable to update: %s \n", e.Error())
	}

	// data := Coba{}
	// data.Id = "1"
	// data.Name = "Doni"
	// e = c.NewQuery().SetConfig("multiexec", false).From("coba").Where(dbox.Eq("id", "1")).Update().Exec(nil)
	// if e != nil {
	// 	t.Errorf("Unable to update: %s \n", e.Error())
	// }

	// data := Coba{}
	// data.Id = "2"
	// data.Name = "wkwkwkwkwkkk"
	// e = c.NewQuery().SetConfig("multiexec", false).From("coba").Where(dbox.Eq("id", "2")).Update().Exec(toolkit.M{"data": data})
	// if e != nil {
	// 	t.Errorf("Unable to update: %s \n", e.Error())
	// }

	// dataWithNoID := NoID{}
	// dataWithNoID.Aidi = "7"
	// dataWithNoID.Nama = "cuma satu lagi"
	// e = c.NewQuery().SetConfig("multiexec", false).From("noid").Update().Exec(toolkit.M{"data": dataWithNoID})
	// if e != nil {
	// 	t.Errorf("Unable to update: %s \n", e.Error())
	// }

	// dataWithNoID := SakID{}
	// dataWithNoID.Aidi = "55"
	// dataWithNoID.Nama = "cuma satu lagi"
	// e = c.NewQuery().SetConfig("multiexec", true).From("noid").Where(dbox.Eq("aidi", "55")).Update().Exec(toolkit.M{"data": dataWithNoID})
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

// func TestSelectFilter(t *testing.T) {
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("Unable to connect %s \n", e.Error())
// 		return
// 	}
// 	defer c.Close()

// 	layoutFormat := "2006-01-02 15:04:05"
// 	dateValue1 := "2016-01-12 07:36:15"
// 	dateValue2 := "2016-01-12 14:36:15"
// 	var tanggal1 time.Time
// 	var tanggal2 time.Time
// 	tanggal1, _ = time.Parse(layoutFormat, dateValue1)
// 	tanggal2, _ = time.Parse(layoutFormat, dateValue2)
// 	fmt.Println(tanggal1, tanggal2)
// 	csr, e := c.NewQuery().
// 		Select("id", "name", "tanggal", "umur").
// 		From("tes").
//Where(dbox.Eq("name", "Bourne")).
//Where(dbox.Ne("name", "Bourne")).
//Where(dbox.Gt("umur", 25)).
//Where(dbox.Gte("umur", 25)).
//Where(dbox.Lt("umur", 25)).
//Where(dbox.Lte("tanggal", tanggal1)).
//Where(dbox.Lte("umur", 25)).
//Where(dbox.In("name", "Roy", "Bourne")).
//Where(dbox.In("umur", 25, 29)).
//Where(dbox.Nin("umur", 25, 30)).
//Where(dbox.In("tanggal", tanggal1, tanggal2)).
//Where(dbox.And(dbox.Gt("umur", 25), dbox.Eq("name", "Roy"))).
//Where(dbox.Contains("name", "ar")).
//Where(dbox.Or(dbox.Contains("name", "oy"), dbox.Contains("name", "au"))).
//Where(dbox.Startwith("name", "Os")).
//Where(dbox.Endwith("name", "ne")).
//Order("name").
// Skip(2).
// Take(1).
//Cursor(nil)
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
// Where(dbox.Contains("name", "@nama")).
// Cursor(toolkit.M{}.Set("nama", "ne"))
// Where(dbox.Startwith("name", "@nama")).
// Cursor(toolkit.M{}.Set("nama", "Os"))
// Where(dbox.Endwith("name", "@nama")).
// Cursor(toolkit.M{}.Set("nama", "ne"))
// Where(dbox.Nin("umur", "@umur1", "@umur2")).
// Cursor(toolkit.M{}.Set("umur1", 25).Set("umur2", 29))
// Where(dbox.Ne("name", "@nama1")).
// Cursor(toolkit.M{}.Set("nama1", "Bourne"))
// 		Where(dbox.Lt("umur", "@umur1")).
// 		Cursor(toolkit.M{}.Set("umur1", "25"))
// 	if e != nil {
// 		t.Errorf("Cursor pre error: %s \n", e.Error())
// 		return
// 	}
// 	if csr == nil {
// 		t.Errorf("Cursor not initialized")
// 		return
// 	}
// 	defer csr.Close()

// 	// results := make([]map[string]interface{}, 0)
// 	results := make([]User, 0)

// 	err := csr.Fetch(&results, 0, false)
// 	if err != nil {
// 		t.Errorf("Unable to fetch: %s \n", err.Error())
// 	} else {
// 		fmt.Println("======================")
// 		fmt.Println("Select with FILTER")
// 		fmt.Println("======================")

// 		fmt.Printf("Fetch N OK. Result:\n")
// 		for i := 0; i < len(results); i++ {
// 			fmt.Printf("%v \n", toolkit.JsonString(results[i]))
// 		}

// 	}
// }
