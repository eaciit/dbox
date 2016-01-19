package json

import (
	"fmt"
	"github.com/eaciit/dbox"
	"github.com/eaciit/toolkit"
	// "io"
	"os"
	"path/filepath"
	"testing"
)

func prepareConnection() (dbox.IConnection, error) {
	config := toolkit.M{"newfile": true} //for create new file, if you dont need just overwrite "config" with "nil"
	wd, _ := os.Getwd()
	ci := &dbox.ConnectionInfo{filepath.Join(wd, "test.json"), "", "", "", config}

	c, e := dbox.NewConnection("json", ci)
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
	}

	defer c.Close()
}

func TestFilter(t *testing.T) {
	fb := dbox.NewFilterBuilder(new(FilterBuilder))
	fb.AddFilter(dbox.Or(
		dbox.Eq("_id", 1),
		dbox.Eq("group", "administrators")))
	b, e := fb.Build()
	if e != nil {
		t.Errorf("Error %s", e.Error())
	} else {
		fmt.Printf("Result:\n%v\n", toolkit.JsonString(b))
	}
}

func TestSelect(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().Select("*").Cursor(nil)

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

	// results := make([]toolkit.M, 0)
	type App struct {
		Id    string `json:"id"`
		Title string `json:"title"`
		Email string `json:"email"`
	}

	var apps []App
	e = csr.Fetch(&apps, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch all: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch all OK. Result: %v \n", len(apps))
	}

	e = csr.ResetFetch()
	if e != nil {
		t.Errorf("Unable to reset fetch: %s \n", e.Error())
	}

	//ds, e = csr.Fetch(nil, 3, false)
	e = csr.Fetch(&apps, 3, false)
	if e != nil {
		t.Errorf("Unable to fetch N: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch N3 OK. Result: %v \n",
			apps)
	}

	e = csr.Fetch(&apps, 4, false)
	if e != nil {
		t.Errorf("Unable to fetch N: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch N4 OK. Result: %v \n",
			apps)
	}
}

func TestSelectAll(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().Cursor(nil)

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

	//ds, e := csr.Fetch(nil, 0, false)
	results := make([]toolkit.M, 0)
	e = csr.Fetch(&results, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch all: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch all fields. Result: %v \n", toolkit.JsonString(results[0]))
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
		//Select("_id", "email").
		Where(dbox.Eq("email", "User-4@myco.com")).
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

	// results := make([]toolkit.M, 0)
	type App struct {
		Id    string `json:"id"`
		Title string `json:"title"`
		Email string `json:"email"`
	}

	var apps []App
	e = csr.Fetch(&apps, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch OK. Result: %v \n",
			toolkit.JsonString(apps[0]))

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
	type user struct {
		Id    string `json:"id"`
		Title string `json:"title"`
		Email string `json:"email"`
	}

	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	e = c.NewQuery().Delete().Exec(nil)
	if e != nil {
		t.Errorf("Unablet to clear table %s\n", e.Error())
		return
	}

	///Test save 10000 datas
	q := c.NewQuery().SetConfig("multiexec", true).Save()
	for i := 1; i <= 10; i++ {
		//go func(q dbox.IQuery, i int) {
		data := user{}
		data.Id = fmt.Sprintf("User-%d", i)
		data.Title = fmt.Sprintf("User-%d's name", i)
		data.Email = fmt.Sprintf("User-%d@myco.com", i)
		if i == 10 || i == 20 || i == 30 {
			data.Email = fmt.Sprintf("User-%d@myholding.com", i)
		}

		e = q.Exec(toolkit.M{
			"data": data,
		})
		if e != nil {
			t.Errorf("Unable to save: %s \n", e.Error())
		}

	}
	q.Close()

	///insert
	dataInsert := user{}
	dataInsert.Id = fmt.Sprintf("User-15")
	dataInsert.Title = fmt.Sprintf("User Lima Belas")
	dataInsert.Email = fmt.Sprintf("user15@yahoo.com")
	e = c.NewQuery().Insert().Exec(toolkit.M{"data": dataInsert})
	if e != nil {
		t.Errorf("Unable to insert: %s \n", e.Error())
	}

	/// Test insert slice
	dataInsertSlice := user{}
	var insertSlice []user
	dataInsertSlice.Id = fmt.Sprintf("User-unknown")
	dataInsertSlice.Title = fmt.Sprintf("User Unknown")
	dataInsertSlice.Email = fmt.Sprintf("userUnknown@yahoo.com")
	insertSlice = append(insertSlice, dataInsertSlice)
	e = c.NewQuery().Insert().Exec(toolkit.M{"data": insertSlice})
	if e != nil {
		t.Errorf("Unable to insert: %s \n", e.Error())
	}

	///update
	/*type field struct {
		Id         int    `json:"_id"`
		DataSource string `json:"dataSource"`
		Field      string `json:"field"`
	}
	type user struct {
		Id               string  `json:"ID"`
		Fields           []field `json:"fields"`
		MasterDataSource string  `json:"masterDataSource"`
		Title            string  `json:"title"`
	}

	data.Id = "1"
	data.Fields = []field{
		{1, "John", "Doe"},
		{2, "Barrack", "Obama"},
	}
	data.MasterDataSource = "master"
	data.Title = "Test update"*/
	dataUpdate := user{}
	dataUpdate.Id = fmt.Sprintf("User-10")
	dataUpdate.Title = fmt.Sprintf("User sepoloh")
	dataUpdate.Email = fmt.Sprintf("user10@yahoo.com")
	e = c.NewQuery().Update().Exec(toolkit.M{"data": dataUpdate})
	if e != nil {
		t.Errorf("Unable to update: %s \n", e.Error())
	}

	///delete with where
	e = c.NewQuery().Where(dbox.Eq("id", "User-1")).Delete().Exec(nil)
	if e != nil {
		t.Errorf("Unablet to delete table %s\n", e.Error())
		return
	}
}

func TestCRUDSaveSameId(t *testing.T) {
	type user struct {
		Id    string `json:"id"`
		Title string `json:"title"`
		Email string `json:"email"`
	}

	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	e = c.NewQuery().Delete().Exec(nil)
	if e != nil {
		t.Errorf("Unablet to clear table %s\n", e.Error())
		return
	}
	q := c.NewQuery().SetConfig("multiexec", true).Save()
	for i := 1; i <= 5; i++ {
		//go func(q dbox.IQuery, i int) {
		data := user{}
		data.Id = fmt.Sprintf("User-%d", 1)
		data.Title = fmt.Sprintf("User-%d's name", i)
		data.Email = fmt.Sprintf("User-%d@myco.com", i)
		if i == 5 {
			data.Email = fmt.Sprintf("User-%d@myholding.com", i)
		}

		e = q.Exec(toolkit.M{
			"data": data,
		})
		if e != nil {
			t.Errorf("Unable to save: %s \n", e.Error())
		}

	}

	for i := 2; i <= 10; i++ {
		//go func(q dbox.IQuery, i int) {
		data := user{}
		data.Id = fmt.Sprintf("User-%d", i)
		data.Title = fmt.Sprintf("User-%d's name", i)
		data.Email = fmt.Sprintf("User-%d@myco.com", i)
		if i == 5 {
			data.Email = fmt.Sprintf("User-%d@myholding.com", i)
		}

		e = q.Exec(toolkit.M{
			"data": data,
		})
		if e != nil {
			t.Errorf("Unable to save: %s \n", e.Error())
		}

	}

	dataSave := user{}
	dataSave.Id = fmt.Sprintf("User-%d", 3)
	dataSave.Title = fmt.Sprintf("User-%d's name", 21)
	dataSave.Email = fmt.Sprintf("User-%d@myco.com", 21)

	e = q.Exec(toolkit.M{
		"data": dataSave,
	})
	if e != nil {
		t.Errorf("Unable to save: %s \n", e.Error())
	}
	q.Close()
}

func TestCRUDSaveNotEmpty(t *testing.T) {
	type user struct {
		Id    string `json:"id"`
		Title string `json:"title"`
		Email string `json:"email"`
	}

	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	q := c.NewQuery().SetConfig("multiexec", true).Save()
	data := user{}
	data.Id = fmt.Sprintf("User-%d", 4)
	data.Title = fmt.Sprintf("User-%d's name", 400)
	data.Email = fmt.Sprintf("User-%d@myco.com", 4)
	e = q.Exec(toolkit.M{
		"data": data,
	})
	if e != nil {
		t.Errorf("Unable to save: %s \n", e.Error())
	}

	data.Id = fmt.Sprintf("User-%d", 99)
	data.Title = fmt.Sprintf("User-%d's name", 99)
	data.Email = fmt.Sprintf("User-%d@myco.com", 99)

	e = q.Exec(toolkit.M{
		"data": data,
	})
	if e != nil {
		t.Errorf("Unable to save: %s \n", e.Error())
	}
	q.Close()
}
