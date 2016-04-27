// com.microsoft.sqlserver.jdbc.SQLServerDriver jdbc:sqlserver://WINDOWS:1433;databaseName=test;user=sa;password=root; sa root

package mssql_test

import (
	"fmt"
	"github.com/eaciit/dbox"
	_ "github.com/eaciit/dbox/dbc/jdbc"
	_ "github.com/eaciit/dbox/dbc/jdbc/driver"
	"github.com/eaciit/toolkit"
	"testing"
)

type User struct {
	Id   int
	Name string
}

func prepareConnection() (dbox.IConnection, error) {
	settings := toolkit.M{"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver", "connector": "jdbc:mssql", "jar": "sqljdbc4-3.0.jar"}
	ci := &dbox.ConnectionInfo{"WINDOWS:1433", "test", "sa", "root", settings}
	c, e := dbox.NewConnection("jdbc", ci)
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
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect: %s \n", e.Error())
	}
	defer c.Close()
}

func TestFetch(t *testing.T) {
	// t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect: %s \n", e.Error())
		toolkit.Println(e)
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Select("name", "id").
		From("dbo.userne").Order("id").
		// Skip(5).
		Take(2).
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

	// e = csr.ResetFetch()
	// if e != nil {
	// 	t.Errorf("Unable to reset fetch: %s \n", e.Error())
	// }

	// err = csr.Fetch(&results, 5, false)
	// if err != nil {
	// 	t.Errorf("Unable to fetch all: %s \n", err.Error())
	// } else {
	// 	toolkit.Println("=========================")
	// 	toolkit.Println("Select Fetch")
	// 	toolkit.Println("=========================")
	// 	toolkit.Println("Fetch N OK. Result:")

	// 	for _, val := range results {
	// 		fmt.Printf("%v \n",
	// 			toolkit.JsonString(val))
	// 	}
	// }

	// err = csr.Fetch(&results, 2, false)
	// if err != nil {
	// 	t.Errorf("Unable to fetch all: %s \n", err.Error())
	// } else {
	// 	toolkit.Println("=========================")
	// 	toolkit.Println("Select Fetch")
	// 	toolkit.Println("=========================")
	// 	toolkit.Println("Fetch N OK. Result:")

	// 	for _, val := range results {
	// 		fmt.Printf("%v \n",
	// 			toolkit.JsonString(val))
	// 	}
	// }
}

func TestCRUD(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect: %s \n", e.Error())
	}
	defer c.Close()

	// ===============================INSERT==============================
	q := c.NewQuery().From("coba").Insert()
	dataInsert := User{}
	dataInsert.Id = 6
	dataInsert.Name = fmt.Sprintf("New Player")

	e = q.Exec(toolkit.M{"data": dataInsert})
	if e != nil {
		t.Errorf("Unable to insert data : %s \n", e.Error())
	}
	// defer q.Close()
}
