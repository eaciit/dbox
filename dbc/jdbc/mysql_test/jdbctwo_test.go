package mysql_test

import (
	"fmt"
	"github.com/eaciit/dbox"
	_ "github.com/eaciit/dbox/dbc/jdbc"
	_ "github.com/eaciit/dbox/dbc/jdbc/driver"
	"github.com/eaciit/toolkit"
	"testing"
)

// Host     string
// 	Database string
// 	UserName string
// 	Password string

// 	Settings toolkit.M

type User struct {
	Id   int
	Name string
	Umur int
}

func prepareConnection() (dbox.IConnection, error) {

	// "tcp://localhost:3306/testgolang?user=root&pass=root&driver=com.mysql.jdbc.Driver&jar=mysql-connector-java-5.1.38-bin.jar&str=jdbc:mysql://localhost:3306/testgolang"

	settings := toolkit.M{"driver": "com.mysql.jdbc.Driver", "connector": "jdbc:mysql", "jar": "mysql-connector-java-5.1.38-bin.jar"}
	ci := &dbox.ConnectionInfo{"localhost:3306", "testgolang", "root", "root", settings}
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
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect: %s \n", e.Error())
		toolkit.Println(e)
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Select("name", "id", "umur").
		From("tes").Order("id").
		Skip(5).
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

func TestRowsTables(t *testing.T) {
	// t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect: %s \n", e.Error())
		toolkit.Println(e)
	}
	defer c.Close()

	csr := c.ObjectNames(dbox.ObjTypeTable)

	for i := 0; i < len(csr); i++ {
		fmt.Printf("show name table %v \n", toolkit.JsonString(csr[i]))
	}
}

func TestCRUD(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect: %s \n", e.Error())
	}
	defer c.Close()

	// ===============================INSERT==============================
	// q := c.NewQuery().From("tes").Insert()
	// dataInsert := User{}
	// dataInsert.Id = 20
	// dataInsert.Name = fmt.Sprintf("New Player")
	// dataInsert.Umur = 40

	// e = q.Exec(toolkit.M{"data": dataInsert})
	// if e != nil {
	// 	t.Errorf("Unable to insert data : %s \n", e.Error())
	// }

	// ===============================SAVE==============================
	// q := c.NewQuery().From("tes").Save()
	// dataInsert := User{}
	// dataInsert.Id = 21
	// dataInsert.Name = fmt.Sprintf("New baru")
	// dataInsert.Umur = 40

	// e = q.Exec(toolkit.M{"data": dataInsert})
	// if e != nil {
	// 	t.Errorf("Unable to insert data : %s \n", e.Error())
	// }

	// ===============================DELETE==============================
	dataInsert := User{}
	dataInsert.Id = 20

	e = c.NewQuery().From("tes").Delete().Exec(toolkit.M{"data": dataInsert})
	if e != nil {
		t.Errorf("Unable to delete data %s\n", e.Error())
		return
	}
}

func TestFreeQuery(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Command("freequery", toolkit.M{}.
			Set("syntax", "select name from tes where name like 'r%'")).
		Cursor(nil)

	if csr == nil {
		t.Errorf("Cursor not initialized", e.Error())
		return
	}
	defer csr.Close()

	results := make([]map[string]interface{}, 0)
	err := csr.Fetch(&results, 0, false)
	if err != nil {
		t.Errorf("Unable to fetch: %s \n", err.Error())
	} else {
		toolkit.Println("======================")
		toolkit.Println("TEST FREE QUERY")
		toolkit.Println("======================")
		toolkit.Println("Fetch N OK. Result: ")
		for _, val := range results {
			toolkit.Printf("%v \n",
				toolkit.JsonString(val))
		}
	}
}
