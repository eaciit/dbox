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

func TestFetch(t *testing.T) {
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

	e = csr.Fetch(&apps, 3, false)
	if e != nil {
		t.Errorf("Unable to fetch N: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch N3 OK. Result: %v \n",
			apps)
	}

	e = csr.Fetch(&apps, 3, false)
	if e != nil {
		t.Errorf("Unable to fetch N: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch N3 OK. Result: %v \n",
			apps)
	}
}

func TestSelectAllField(t *testing.T) {
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
	results := []toolkit.M{} //make([]toolkit.M, 0)
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
		fmt.Printf("Fetch filter. Result: %v \n",
			toolkit.JsonString(apps[0]))

	}
}

func TestSelectParm(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	csr, e := c.NewQuery().
		// Where(dbox.Ne("id", "@userid1")).
		// Cursor(toolkit.M{}.Set("@userid1", "User-4"))
		// Where(dbox.Gt("id", "@userid1")).
		// Cursor(toolkit.M{}.Set("@userid1", "User-4"))
		// Where(dbox.Gte("id", "@userid1")).
		// Cursor(toolkit.M{}.Set("@userid1", "User-4"))
		// Where(dbox.Lt("id", "@userid1")).
		// Cursor(toolkit.M{}.Set("@userid1", "User-4"))
		// Where(dbox.Lte("id", "@userid1")).
		// Cursor(toolkit.M{}.Set("@userid1", "User-4"))
		// Where(dbox.In("id", "@userid1", "@userid2")).
		// Cursor(toolkit.M{}.Set("@userid1", "User-4").Set("@userid2", "User-5"))
		// Where(dbox.Nin("id", "@userid1", "@userid2")).
		// Cursor(toolkit.M{}.Set("@userid1", "User-4").Set("@userid2", "User-5"))
		// Where(dbox.Eq("id", "@userid1")).
		// Cursor(toolkit.M{}.Set("@userid1", "User-4"))
		// Where(dbox.And(dbox.Eq("id", "@userid1"), dbox.Eq("title", "@userid2"))).
		// Cursor(toolkit.M{}.Set("@userid1", "User-4").Set("@userid2", "User-400's name"))
		// Where(dbox.Lt("title", "@title")).
		// Cursor(toolkit.M{}.Set("@title", "User-8's name"))
		// Where(dbox.Or(dbox.Eq("id", "@userid1"), dbox.Eq("id", "@userid2"))).
		// Cursor(toolkit.M{}.Set("@userid1", "User-4").Set("@userid2", "User-5"))
		Where(dbox.Contains("title", "@userid1", "@userid2")).
		Cursor(toolkit.M{}.Set("@userid1", "Own").Set("@userid2", "Lima"))
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
		fmt.Printf("Fetch filter params. Result: %v \n",
			toolkit.JsonString(apps))

	}
}

func TestSelectSkip(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Skip(3).
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

	results := []toolkit.M{}
	e = csr.Fetch(&results, 4, false)
	if e != nil {
		t.Errorf("Unable to fetch: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch skip N4. Result: %v \n",
			toolkit.JsonString(results))

	}

	e = csr.Fetch(&results, 4, false)
	if e != nil {
		t.Errorf("Unable to fetch: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch skip N4. Result: %v \n",
			toolkit.JsonString(results))

	}
}

func TestSelectTake(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Take(5).
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

	results := []toolkit.M{}
	e = csr.Fetch(&results, 3, false)
	if e != nil {
		t.Errorf("Unable to fetch: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch take N3. Result: %v \n",
			toolkit.JsonString(results))

	}

	e = csr.Fetch(&results, 3, false)
	if e != nil {
		t.Errorf("Unable to fetch: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch take 3. Result: %v \n",
			toolkit.JsonString(results))

	}
}

/*func TestSelectSort(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Order("Age").
		Take(15).
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

	results := []toolkit.M{}
	e = csr.Fetch(&results, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch order. Result: %v \n",
			toolkit.JsonString(results))

	}
}*/

/*func TestCount(t *testing.T) {
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

	count := csr.Count()
	fmt.Printf("Count data. Result: %v \n", count)
}
*/
func TestDeleteAll(t *testing.T) {
	// t.Skip()
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
}

type user struct {
	Id    string `json:"id"`
	Title string `json:"title"`
	Email string `json:"email"`
}

func TestSave(t *testing.T) {
	// t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

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
}

func TestInsert(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	///insert
	dataInsert := user{}
	dataInsert.Id = fmt.Sprintf("User-15")
	dataInsert.Title = fmt.Sprintf("User Lima Belas")
	dataInsert.Email = fmt.Sprintf("user15@yahoo.com")
	e = c.NewQuery().Insert().Exec(toolkit.M{"data": dataInsert})
	if e != nil {
		t.Errorf("Unable to insert struct: %s \n", e.Error())
	}

	/// insert slice
	dataInsertSlice := user{}
	var insertSlice []user
	dataInsertSlice.Id = fmt.Sprintf("User-unknown")
	dataInsertSlice.Title = fmt.Sprintf("User Unknown")
	dataInsertSlice.Email = fmt.Sprintf("userUnknown@yahoo.com")
	insertSlice = append(insertSlice, dataInsertSlice)
	e = c.NewQuery().Insert().Exec(toolkit.M{"data": insertSlice})
	if e != nil {
		t.Errorf("Unable to insert slice: %s \n", e.Error())
	}
}

func TestUpdateFilter(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	dataUpdate := user{}
	dataUpdate.Id = fmt.Sprintf("User-10")
	dataUpdate.Title = fmt.Sprintf("User sepoloh")
	dataUpdate.Email = fmt.Sprintf("user10@yahoo.com")

	///update $eq = Equal (==)
	e = c.NewQuery().Update().Where(dbox.Eq("id", "User-1")).Exec(toolkit.M{}.Set("data", toolkit.M{}.Set("title", "Master")))
	if e != nil {
		t.Errorf("Unable to update filter: %s \n", e.Error())
	}

	// ///update $lte = Less than or equal (<=)
	// e = c.NewQuery().Update().Where(dbox.Lte("id", "User-2")).Exec(toolkit.M{}.Set("data", toolkit.M{}.Set("title", false)))
	// if e != nil {
	// 	t.Errorf("Unable to update filter: %s \n", e.Error())
	// }

	// ///update $ne = Not equal (!=)
	// e = c.NewQuery().Update().Where(dbox.Ne("id", "User-3")).Exec(toolkit.M{}.Set("data", toolkit.M{}.Set("title", " - ")))
	// if e != nil {
	// 	t.Errorf("Unable to update filter: %s \n", e.Error())
	// }

	// ///update $gt = Greater than (>)
	// e = c.NewQuery().Update().Where(dbox.Gt("id", "User-4")).Exec(toolkit.M{}.Set("data", toolkit.M{}.Set("title", "Unknown")))
	// if e != nil {
	// 	t.Errorf("Unable to update filter: %s \n", e.Error())
	// }
}

func TestUpdateNoFilter(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	dataUpdate := user{}
	dataUpdate.Id = fmt.Sprintf("User-9")
	dataUpdate.Title = fmt.Sprintf("User semiblan")
	dataUpdate.Email = fmt.Sprintf("userSembilan@yahoo.com")
	e = c.NewQuery().Update().Exec(toolkit.M{"data": dataUpdate})
	if e != nil {
		t.Errorf("Unable to update without filter: %s \n", e.Error())
	}
}

func TestDeleteFilter(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	e = c.NewQuery().Where(dbox.Eq("id", "User-1")).Delete().Exec(nil)
	if e != nil {
		t.Errorf("Unablet to delete filter %s\n", e.Error())
		return
	}
}

func TestSaveSameId(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

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
			t.Errorf("Unable to save same id's: %s \n", e.Error())
		}

	}

	for i := 2; i <= 10; i++ {
		//go func(q dbox.IQuery, i int) {
		data := user{}
		data.Id = fmt.Sprintf("User-%d", i)
		data.Title = fmt.Sprintf("User-%d's name", i)
		data.Email = fmt.Sprintf("User-%d@myco.com", i)
		if i == 7 {
			data.Email = fmt.Sprintf("User-%d@myholding.com", i)
		}

		e = q.Exec(toolkit.M{
			"data": data,
		})
		if e != nil {
			t.Errorf("Unable to save same id's: %s \n", e.Error())
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
		t.Errorf("Unable to save same id's: %s \n", e.Error())
	}
	q.Close()
}

func TestSaveNotEmpty(t *testing.T) {
	t.Skip()
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

func TestUpdateMultiExec(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	e = c.NewQuery().Update().
		SetConfig("multiexec", false).
		Where(dbox.Eq("email", "User-1@myco.com")).
		Exec(toolkit.M{}.Set("data", toolkit.M{}.Set("email", "Master")))
	if e != nil {
		t.Errorf("Unable to update filter: %s \n", e.Error())
	}
}

func TestDeleteMultiExec(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	e = c.NewQuery().
		Where(dbox.Eq("email", "User-1@myco.com")).
		SetConfig("multiexec", false).
		Delete().Exec(nil)
	if e != nil {
		t.Errorf("Unablet to delete filter %s\n", e.Error())
		return
	}
}

/*type testUser struct {
	ID       string `json:"_id"`
	FullName string
	Age      int
	Enable   bool
}

func TestUpdateLte(t *testing.T) {
	ctx, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer ctx.Close()

	e = ctx.NewQuery().Delete().SetConfig("multiexec", true).Exec(nil)
	if e != nil {
		t.Fatalf("Delete fail: %s", e.Error())
	}

	es := []string{}
	qinsert := ctx.NewQuery().SetConfig("multiexec", true).Insert()
	for i := 1; i <= 500; i++ {
		u := &testUser{
			toolkit.Sprintf("user%d", i),
			toolkit.Sprintf("User %d", i),
			toolkit.RandInt(30) + 20, true}
		e = qinsert.Exec(toolkit.M{}.Set("data", u))
		if e != nil {
			es = append(es, toolkit.Sprintf("Insert fail %d: %s", i, e.Error()))
		}
	}

	if len(es) > 0 {
		t.Fatal(es)
	}

	e = ctx.NewQuery().Update().Where(dbox.Lte("_id", "user200")).Exec(toolkit.M{}.Set("data", toolkit.M{}.Set("Enable", false)))
	if e != nil {
		t.Fatalf("Update fail: %s", e.Error())
	}
}*/
