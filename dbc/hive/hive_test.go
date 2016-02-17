package hive

import (
	"fmt"
<<<<<<< HEAD
	"github.com/eaciit/dbox"
=======
<<<<<<< HEAD
=======
	"github.com/ranggadablues/dbox"
>>>>>>> bbe204ed9e388ba424883a5ce94877c03ef0bba5
>>>>>>> refs/remotes/origin/master
	"github.com/eaciit/toolkit"
	"github.com/rinosukmandityo/dbox"
	"testing"
)

type Sample7 struct {
	Code        string `tag_name:"code"`
	Description string `tag_name:"description"`
	Total_emp   string `tag_name:"total_emp"`
	Salary      string `tag_name:"salary"`
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
