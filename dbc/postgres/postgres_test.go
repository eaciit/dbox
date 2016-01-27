package postgres

import (
	"fmt"
	"github.com/eaciit/dbox"
	"github.com/eaciit/toolkit"
	"testing"
	//"time"
)

func prepareConnection() (dbox.IConnection, error) {
	ci := &dbox.ConnectionInfo{"localhost:5432", "test", "postgres", "budi123", nil}
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

/*func TestSelectFilter(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Select().
		Where(dbox.Eq("id", "3456")).
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

	results := make([]map[string]interface{}, 0)

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
}*/

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

///*=================================================test procedures=====================================================*/

// func TestProcedures(t *testing.T) {
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("unable to connect %s \n", e.Error())
// 		return
// 	}
// 	defer c.Close()

///*================================================function using parameters===========================================*/

// 	csr, e := c.NewQuery().Command("procedure", toolkit.M{}.Set("name", "add_name").Set("parms", toolkit.M{}.Set("@id", "333").Set("@name", "siena"))).Cursor(nil)

///*=================================================function no parameters=============================================*/

// 	//csr, e := c.NewQuery().Command("procedure", toolkit.M{}.Set("name", "show_again").Set("parms", toolkit.M{}.Set("", ""))).Cursor(nil)

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

// func TestSelect(t *testing.T) {
// 	c, e := prepareConnection()
// 	if e != nil {
// 		t.Errorf("unable to connect %s \n", e.Error())
// 		return
// 	}

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

func TestStartorEndWith(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("unnable  to connect %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().Select("id", "name", "umur").From("tes").Where(dbox.Startwith("name", "Bo")).Cursor(nil)
	//csr, e := c.NewQuery().Select("id", "name", "umur").From("tes").Where(dbox.Endwith("name", "ar")).Cursor(nil)

	if e != nil {
		t.Errorf("cursor pre error : %s \n", e.Error())
		return
	}

	if csr == nil {
		t.Errorf("cursor not initialized")
	}

	results := make([]map[string]interface{}, 0)

	err := csr.Fetch(&results, 0, false)
	if err != nil {
		t.Errorf("unnable to fetch: %s \n", err.Error())
	} else {
		fmt.Println("===========================")
		fmt.Println("contain data")
		fmt.Println("===========================")

		fmt.Println("fetch N Ok. Result :\n")

		for i := 0; i < len(results); i++ {
			fmt.Printf("%v \n", toolkit.JsonString(results[i]))
		}
	}

}
