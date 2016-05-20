package othertest

// import (
// 	"database/sql"
// 	// "fmt"
// 	"github.com/eaciit/toolkit"
// 	_ "go-odbc/driver"
// 	"testing"
// )

// func TestSql(t *testing.T) {
// 	db, err := sql.Open("odbc", "DSN=mysql-dsn;UID=root;PWD=root")
// 	if err != nil {
// 		toolkit.Println("Error open SQL:", err.Error())
// 	}
// 	defer db.Close()

// 	stmt, err := db.Prepare("select productname from products")
// 	if err != nil {
// 		toolkit.Println("Error stmt:", err.Error())
// 	}
// 	defer stmt.Close()

// 	rows, err := stmt.Query()
// 	if err != nil {
// 		toolkit.Println("Error stmt query:", err.Error())
// 	}
// 	defer rows.Close()

// 	for rows.Next() {
// 		var name string

// 		_ = rows.Scan(&name)
// 		// toolkit.Println(name)

// 		toolkit.Printf("%v\n", name)
// 	}

// }

import (
	"github.com/eaciit/toolkit"
	"odbc"
	"testing"
)

func TestOdbc(t *testing.T) {
	conn, e := odbc.Connect("DSN=mysql-dsn;UID=root;PWD=root")
	if e != nil {
		toolkit.Println("Conn Error found: ", e.Error())
	}
	toolkit.Println(conn)
	query := "SELECT id, name FROM dummy"
	stmt, e := conn.Prepare(query)
	if e != nil {
		toolkit.Println("STMT Error found: ", e.Error())
	}
	// toolkit.Println(conn, stmt)
	e = stmt.Execute()
	if e != nil {
		toolkit.Println("Execute Error found: ", e.Error())
	}

	rows, e := stmt.FetchAll()
	if e != nil {
		toolkit.Println("FetchAll Error found: ", e.Error())
	}
	// toolkit.Println(toolkit.JsonString(rows))
	for _, row := range rows {
		toolkit.Printf("%v\n", row.Data)
	}

	stmt.Close()
	conn.Close()
}
