package dbox_test

import (
	"fmt"
	"github.com/eaciit/dbox"
	"github.com/eaciit/dbox/dbc/mongo"
	"testing"
)

var ctx IConnection

func connect() error {
	return nil
}

func close() {
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
	fmt.Println("Testing connection")
	e := connect()
	if e != nil {
		t.Errorf("Error connecting to database: %s \n", e.Error())
	}

	defer close()

	cursor, e := ctx.NewQuery().Select("_id", "title").From("testtable").
		Where(ctx.Or(ctx.Eq("_id", 20), ctx.Eq("title", "default"))).
		Cursor()
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
