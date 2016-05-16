package oracle

import (
	"github.com/eaciit/dbox"
	"github.com/eaciit/toolkit"
	"testing"
	"time"
)

const (
	tableName      string = "emp"
	tableCustomers string = "customers"
	tableProducts  string = "products"
)

type emp struct {
	Empno    int
	Ename    string
	Job      string
	Mgr      int32
	HireDate time.Time
	Sal      int
	Comm     int
	DeptNo   int
}

type customers struct {
	Id    int
	Name  string
	Phone string
	Email string
	Date  time.Time
}

type products struct {
	Id          int
	Productname string
	Productcode string
	DATE        time.Time
}

func prepareConnection() (dbox.IConnection, error) {
	ci := &dbox.ConnectionInfo{"localhost:1521/xe", "dboxtest", "dboxtest", "root", nil}
	c, e := dbox.NewConnection("oracle", ci)
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
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect: %s \n", e.Error())
		toolkit.Println(e)
	} else {
		toolkit.Println("###########################")
		toolkit.Println("Horay, connection success!")
		toolkit.Println("###########################")
	}
	defer c.Close()
}

func TestSelect(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()

	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	// csr, e := c.NewQuery().Select().From("tes").Where(dbox.Eq("id", "3")).Cursor(nil)
	csr, e := c.NewQuery().
		// Select("empno", "ename", "hiredate").
		From(tableCustomers).Cursor(nil)

	if e != nil {
		t.Errorf("Cursor pre error: %s \n", e.Error())
		return
	}
	if csr == nil {
		t.Errorf("Cursor not initialized")
		return
	}
	defer csr.Close()

	rets := []toolkit.M{}
	e = csr.Fetch(&rets, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch N: %s \n", e.Error())
	} else {
		toolkit.Printf("Fetch N OK. Result: %v \n", toolkit.JsonString(rets))
		toolkit.Printf("Total Fetch OK : %v \n", toolkit.SliceLen(rets))
	}
}

func TestSelectFilter(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Select("empno", "ename", "mgr", "hiredate").
		Where(dbox.Or(dbox.Eq("empno", 7521), dbox.Eq("ename", "ADAMS"))).
		From(tableName).Cursor(nil)
	if e != nil {
		t.Errorf("Cursor pre error: %s \n", e.Error())
		return
	}
	if csr == nil {
		t.Errorf("Cursor not initialized")
		return
	}
	defer csr.Close()

	rets := /*[]customers{}*/ []toolkit.M{}
	e = csr.Fetch(&rets, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch: %s \n", e.Error())
	} else {
		toolkit.Printf("Filter OK. Result: %v \n", toolkit.JsonString(rets))
	}
}

func TestSelectAggregate(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Select("productname").
		Aggr(dbox.AggrSum, "price", "Total Price").
		Aggr(dbox.AggrAvr, "price", "Avg").
		From(tableProducts).
		Group("productname").
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

	rets := []toolkit.M{}
	e = csr.Fetch(&rets, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch: %s \n", e.Error())
	} else {
		toolkit.Printf("Fetch OK. Result: %v \n", toolkit.JsonString(rets))

	}
}

func TestInsert(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	/*===============================INSERT==============================*/
	/*q := c.NewQuery().From(tableCustomers).Insert()
	data := customers{}
	data.Id = 7
	data.Name = "Bejo"
	data.Phone = "(08) 1234 5678"
	data.Email = "Kuli@kuli.crot"
	data.Date = toolkit.String2Date("02-FEB-1902", "dd-MMM-YYYY")*/

	q := c.NewQuery().From(tableProducts).Insert()
	data := products{}
	data.Id = 1
	data.Productname = "Joko"
	data.Productcode = "000 1234 5678"
	data.DATE = toolkit.String2Date("02-FEB-1002", "dd-MMM-YYYY")

	e = q.Exec(toolkit.M{"data": data})
	if e != nil {
		t.Errorf("Unable to insert data : %s \n", e.Error())
	}
}

func TestUpdate(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	/*data := products{}
	data.Id = 1
	data.Productname = "Hendro"
	data.Productcode = "0007 2189 9383"
	data.DATE = toolkit.String2Date("12-NOV-2014", "dd-MMM-YYYY")
	e = c.NewQuery().From(tableProducts).Where(dbox.Eq("id", 1)).Update().Exec(toolkit.M{"data": data})
	if e != nil {
		t.Errorf("Unable to update: %s \n", e.Error())
	}*/

	/*================ update WITHOUT where ===================================================*/

	data := products{}
	data.Id = 2
	data.Productname = "Paijo"
	data.Productcode = "0007 0012 0090"
	data.DATE = toolkit.String2Date("12-NOV-2010", "dd-MMM-YYYY")
	e = c.NewQuery().From(tableProducts).Update().Exec(toolkit.M{"data": data})
	if e != nil {
		t.Errorf("Unable to update: %s \n", e.Error())
	}
}

func TestSave(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	q := c.NewQuery().From(tableProducts).Save()
	data := products{}
	data.Id = 2
	data.Productname = "Jono"
	data.Productcode = "0007 0012 0090"
	data.DATE = toolkit.String2Date("12-NOV-2010", "dd-MMM-YYYY")
	e = q.Exec(toolkit.M{"data": data})
	if e != nil {
		t.Errorf("Unable to save data : %s \n", e.Error())
	}

	data.Id = 3
	data.Productname = "Jarwo"
	data.Productcode = "1111 2222 3333"
	data.DATE = toolkit.String2Date("21-May-1998", "dd-MMM-YYYY")
	e = q.Exec(toolkit.M{"data": data})
	if e != nil {
		t.Errorf("Unable to save data : %s \n", e.Error())
	}
}

func TestDelete(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
		return
	}
	defer c.Close()

	e = c.NewQuery().From(tableProducts).Where(dbox.And(dbox.Eq("id", "1"), dbox.Eq("productname", "Hendro"))).Delete().Exec(nil)
	if e != nil {
		t.Errorf("Unable to delete data %s\n", e.Error())
		return
	}

	/*data := products{}
	data.Id = 2

	e = c.NewQuery().From(tableProducts).Delete().Exec(toolkit.M{"data": data})
	if e != nil {
		t.Errorf("Unable to delete data %s\n", e.Error())
		return
	}*/
}

func TestTakeSkip(t *testing.T) {
	t.Skip()
	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Select("id", "productname").
		From(tableProducts).
		Take(5).
		Skip(10).
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

	rets := []toolkit.M{}
	e = csr.Fetch(&rets, 0, false)
	if e != nil {
		t.Errorf("Unable to fetch: %s \n", e.Error())
	} else {
		toolkit.Printf("Fetch OK. Result: %v \n", toolkit.JsonString(rets))
		toolkit.Printf("Total Record OK. Result: %v \n", toolkit.SliceLen(rets))

	}
}

func TestProcedure(t *testing.T) {
	t.Skip()

	c, e := prepareConnection()
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}
	defer c.Close()

	csr, e := c.NewQuery().
		Command("procedure", toolkit.M{}.
			Set("name", "testSPWithParam").
			Set("orderparam", []string{"@param1"}).
			Set("parms", toolkit.M{}.Set("@param1", "test"))).
		Cursor(nil)
	if e != nil {
		t.Errorf("Unable to connect %s \n", e.Error())
	}

	if csr == nil {
		t.Errorf("Cursor not initialized", e.Error())
		return
	}
	defer csr.Close()

	results := make([]map[string]interface{}, 0)

	err := csr.Fetch(&results, 0, false)
	if err != nil {
		t.Errorf("Unable to fetch: %s \n", err.Error())
	} else {
		TestSelect(t)
	}
}
