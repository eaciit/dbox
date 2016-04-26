package jdbc

import (
	"database/sql"
	"fmt"
	_ "github.com/japettyjohn/go-jdbc"
	// "sync"
	"testing"
	// "time"
)

// const testConnString = "tcp://localhost:3306/testgolang?user=root&password=root"
const testConnString = "tcp://localhost:3306"

// const testConnString = "root:root@tcp(127.0.0.1:3306)/testgolang"

type Test struct {
	Id   int64
	Name string
}

func TestJDBCBasic(t *testing.T) {
	db, err := sql.Open("jdbc", testConnString)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	// if status, e := ServerStatus(testConnString); e != nil {
	// 	t.Fatal(e)
	// } else {
	// 	t.Log(status)
	// }

	// sqld, err := db.Prepare("select * from tes")
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// defer sqld.Close()

	// row, err := sqld.Query()
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// defer row.Close()

	// for row.Next() {
	// 	var data string
	// 	row.Scan(&data)
	// 	fmt.Println(data)
	// }
}
