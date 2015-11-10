package dbox

import (
	"fmt"
	"testing"
)

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
