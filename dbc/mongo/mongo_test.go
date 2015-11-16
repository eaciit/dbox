package mongo

import (
	"fmt"

	"github.com/eaciit/dbox"
	"github.com/eaciit/toolkit"
	"testing"
)

func prepareConnection() (dbox.IConnection, error) {
	c, e := dbox.NewConnection("mongo", &dbox.ConnectionInfo{"localhost:27123", "eccolony", "", "", nil})
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
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().Select("_id", "email").From("appusers").
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

	//rets := []toolkit.M{}

	ds, e := csr.Fetch(nil, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch all: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch all OK. Result: %d \n", len(ds.Data))
	}

	e = csr.ResetFetch()
	if e != nil {
		t.Errorf("Unable to reset fetch: %s \n", e.Error())
	}

	ds, e = csr.Fetch(nil, 3, false)
	if e != nil {
		t.Errorf("Unable to fetch N: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch N OK. Result: %v \n",
			ds.Data)
	}
}

func TestSelectFilter(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().
		//Select("_id", "email").
		Where(c.Fb().Eq("email", "arief@eaciit.com")).
		From("appusers").Cursor(nil)
	if e != nil {
		t.Errorf("Cursor pre error: %s \n", e.Error())
		return
	}
	if csr == nil {
		t.Errorf("Cursor not initialized")
		return
	}
	defer csr.Close()

	//rets := []toolkit.M{}

	ds, e := csr.Fetch(nil, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch OK. Result: %v \n",
			toolkit.JsonString(ds.Data[0]))

	}
}

/*
func TestSelectAggregate(t *testing.T) {
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	fb := c.Fb()
	csr, e := c.NewQuery().
		//Select("_id", "email").
		//Where(c.Fb().Eq("email", "arief@eaciit.com")).
		Aggr(dbox.AggSum, 1, "Count").
		Aggr(dbox.AggSum, 1, "Avg").
		From("appusers").
		Group("").
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

	//rets := []toolkit.M{}

	ds, e := csr.Fetch(nil, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch: %s \n", e.Error())
	} else {
		fmt.Printf("Fetch OK. Result: %v \n",
			toolkit.JsonString(ds.Data[0]))

	}
}
*/
