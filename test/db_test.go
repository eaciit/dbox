package dbox_test

import (
	"fmt"

	"github.com/eaciit/dbox"
	_ "github.com/eaciit/dbox/dbc/mongo"
	"github.com/eaciit/toolkit"
	"testing"
)

var ctx dbox.IConnection

func connect() error {
	var e error
	if ctx == nil {
		ctx, e = dbox.NewConnection("mongo",
			&dbox.ConnectionInfo{"localhost:27123", "ectest", "", "", nil})
		if e != nil {
			return e
		}
	}
	e = ctx.Connect()
	return e
}

func close() {
	if ctx != nil {
		ctx.Close()
	}
}

func TestConnect(t *testing.T) {
	fmt.Println("Testing connection")
	e := connect()
	if e != nil {
		t.Errorf("Error connecting to database: %s \n", e.Error())
	}

	defer close()
}

func TestQuery(t *testing.T) {
	fmt.Println("Testing Query")
	e := connect()
	if e != nil {
		t.Errorf("Error connecting to database: %s \n", e.Error())
	}
	defer close()

	fb := ctx.Fb()
	cursor, e := ctx.NewQuery().Select("_id", "title").From("testtable").
		Where(fb.Or(fb.Eq("_id", 20), fb.Eq("title", "default"))).
		Cursor(nil)
	if e != nil {
		t.Errorf("Unable to generate cursor. %s", e.Error())
	}
	defer cursor.Close()

	results := make([]toolkit.M, 0)
	e = cursor.Fetch(results, 0, false)
	if e != nil {
		t.Errorf("Unable to iterate cursor %s", e.Error())
	}
}
