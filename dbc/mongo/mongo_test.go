package mongo

import (
	"fmt"

	"github.com/eaciit/dbox"
	"github.com/eaciit/toolkit"
	"testing"
)

func prepareConnection() (dbox.IConnection, error) {
	c, e := dbox.NewConnection("mongo", &dbox.ConnectionInfo{"localhost:27123", "ectest", "", "", nil})
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
	}
	defer c.Close()
}

func TestFilter(t *testing.T) {
	fb := dbox.NewFilterBuilder(new(FilterBuilder))
	fb.AddFilter(fb.Or(
		fb.Eq("_id", 1),
		fb.Eq("group", "administrators")))
	b, e := fb.Build()
	if e != nil {
		t.Errorf("Error %s", e.Error())
	} else {
		fmt.Printf("Result:\n%v\n", toolkit.JsonString(b))
	}
}

func TestSelect(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connet %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().Select("_id", "email").From("appusers").Take(5).Cursor(nil)
	if e != nil {
		t.Errorf("Cursor pre error: %s \n", e.Error())
		return
	}
	if csr == nil {
		t.Errorf("Cursor not initialized")
		return
	}

	rets := []toolkit.M{}
	e = csr.Fetch(rets, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch: %s \n", e.Error())
	}

	fmt.Printf("Result:\n%s\n", toolkit.JsonString(rets))
}
