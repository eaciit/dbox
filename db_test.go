package dbox

import (
	"./dbc/mongo"
	"fmt"
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

	cursor, error := ctx.NewQuery().Cursor()
}
