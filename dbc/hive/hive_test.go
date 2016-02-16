package hive

import (
	"fmt"
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
	// ci := &dbox.ConnectionInfo{"localhost:3306", "test", "root", "root", nil}
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
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect: %s \n", e.Error())
		fmt.Println(e)
	} else {
		fmt.Println(c)
	}
	// defer c.Close()
}

func TestSelect(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	} else {
		fmt.Println(c)
	}

	// defer c.Close()

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

	// var DoSomething = func(res string) {
	// 	tmp := Sample7{}
	// 	h.ParseOutput(res, &tmp)
	// 	fmt.Println(tmp)
	// }

	// e = h.ExecLine(q, DoSomething)
	// fmt.Printf("error: \n%v\n", e)
	// defer csr.Close()

	// // results := make([]map[string]interface{}, 0)
	results := make([]Sample7, 0)

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

	// if e != nil {
	// 	t.Errorf("Cursor pre error: %s \n", e.Error())
	// 	return
	// }
	// if csr == nil {
	// 	t.Errorf("Cursor not initialized")
	// 	return
	// }
	// defer csr.Close()

	// // results := make([]map[string]interface{}, 0)
	// results := make([]User, 0)

	// err := csr.Fetch(&results, 0, false)
	// if err != nil {
	// 	t.Errorf("Unable to fetch: %s \n", err.Error())
	// } else {
	// 	fmt.Println("======================")
	// 	fmt.Println("Select with FILTER")
	// 	fmt.Println("======================")

	// 	fmt.Printf("Fetch N OK. Result:\n")
	// 	for i := 0; i < len(results); i++ {
	// 		fmt.Printf("%v \n", toolkit.JsonString(results[i]))
	// 	}

	// }
}
