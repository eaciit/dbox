package odbc

import (
	"errors"
	"github.com/eaciit/dbox"
	err "github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"odbc"
)

const (
	modCursor = "Cursor"

	QueryResultCursor = "SQLCursor"
	QueryResultPipe   = "SQLPipe"
)

type Cursor struct {
	dbox.Cursor
	count, start int
	data         toolkit.Ms
	Sess         *odbc.Connection
}

func (c *Cursor) Count() int {
	return c.count
}

func (c *Cursor) ResetFetch() error {
	c.start = 0
	return nil
}

func (c *Cursor) Fetch(m interface{}, n int, closeWhenDone bool) error {
	end := c.start + n
	if end > c.count || n == 0 {
		end = c.count
	}

	if c.start >= c.count {
		return errors.New("No more data to fetched!")
	}
	e := toolkit.Serde(c.data[c.start:end], m, "json")
	if e != nil {
		return err.Error(packageName, modCursor, "Fetch", e.Error())
	}
	// toolkit.Println(c.data)

	/*var js = `[{"Discontinued":false,"ProductID":1,"ProductName":"Chai","UnitPrice":18,"UnitsInStock":39},{"Discontinued":false,"ProductID":2,"ProductName":"Chang","UnitPrice":19,"UnitsInStock":17},{"Discontinued":false,"ProductID":3,"ProductName":"Aniseed Syrup","UnitPrice":10,"UnitsInStock":13},{"Discontinued":false,"ProductID":4,"ProductName":"Chef Anton's Caju","UnitPrice":22,"UnitsInStock":53},{"Discontinued":true,"ProductID":5,"ProductName":"Chef Anton's Gumb","UnitPrice":21.35,"UnitsInStock":0},{"Discontinued":false,"ProductID":6,"ProductName":"Grandma's Boysenb","UnitPrice":25,"UnitsInStock":120},{"Discontinued":false,"ProductID":7,"ProductName":"Uncle Bob's Organ","UnitPrice":30,"UnitsInStock":15},{"Discontinued":false,"ProductID":8,"ProductName":"Northwoods Cranbe","UnitPrice":40,"UnitsInStock":6},{"Discontinued":true,"ProductID":9,"ProductName":"Mishi Kobe Niku","UnitPrice":97,"UnitsInStock":29},{"Discontinued":false,"ProductID":10,"ProductName":"Ikura","UnitPrice":31,"UnitsInStock":31}]`
	result := toolkit.Ms{}
	toolkit.UnjsonFromString(js, &result)
	e := toolkit.Serde(result[0:1], m, "json")
	if e != nil {
		return err.Error(packageName, modCursor, "Fetch", e.Error())
	}*/
	return nil
}

func (c *Cursor) Close() {
	c.Sess.Close()
}
