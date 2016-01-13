package dbox_test

import (
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

func skipIfConnectionIsNil(t *testing.T) {
	if ctx == nil {
		t.Skip()
	}
}

func TestConnect(t *testing.T) {
	e := connect()
	if e != nil {
		t.Errorf("Error connecting to database: %s \n", e.Error())
	}
}

const (
	tableName string = "TestUsers"
)

type testUser struct {
	ID       string `bson:"_id",json:"_id"`
	FullName string
	Age      int
	Enable   bool
}

func TestCRUD(t *testing.T) {
	skipIfConnectionIsNil(t)
	e := ctx.NewQuery().Delete().From(tableName).SetConfig("multiexec", true).Exec(nil)
	if e != nil {
		t.Fatalf("Delete fail: %s", e.Error())
	}

	es := []string{}
	qinsert := ctx.NewQuery().From(tableName).SetConfig("multiexec", true).Insert()
	for i := 1; i <= 500; i++ {
		u := &testUser{
			toolkit.Sprintf("user%d", i),
			toolkit.Sprintf("User %d", i),
			toolkit.RandInt(30) + 20, true}
		e = qinsert.Exec(toolkit.M{}.Set("data", u))
		if e != nil {
			es = append(es, toolkit.Sprintf("Insert fail %d: %s", i, e.Error()))
		}
	}

	if len(es) > 0 {
		t.Fatal(es)
	}

	e = ctx.NewQuery().Update().From(tableName).Where(dbox.Lte("_id", "user200")).Exec(toolkit.M{}.Set("data", toolkit.M{}.Set("enable", false)))
	if e != nil {
		t.Fatalf("Update fail: %s", e.Error())
	}
}

func TestQueryAggregate(t *testing.T) {
	skipIfConnectionIsNil(t)
	cursor, e := ctx.NewQuery().From(tableName).
		//Where(dbox.Lte("_id", "user600")).
		Aggr(dbox.AggrSum, 1, "Count").
		Aggr(dbox.AggrAvr, "$age", "AgeAverage").
		Group("enable").
		Cursor(nil)
	if e != nil {
		t.Errorf("Unable to generate cursor. %s", e.Error())
	}
	defer cursor.Close()

	//results := make([]toolkit.M, 0)
	ds, e := cursor.Fetch(nil, 0, false)
	if e != nil {
		t.Errorf("Unable to iterate cursor %s", e.Error())
	} else {
		toolkit.Printf("Result:\n%s\n", toolkit.JsonString(ds.Data))
	}
}

func TestClose(t *testing.T) {
	skipIfConnectionIsNil(t)
	ctx.Close()
}
