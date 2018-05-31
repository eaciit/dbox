package mongo

// func TestConnect(t *testing.T) {
//  c, e := prepareConnection()
//  if e != nil {
//      t.Fatalf("Unable to connect: %s \n", e.Error())
//      return
//  }
//  defer c.Close()
// }

// func TestFilter(t *testing.T) {
//  fb := db.NewFilterBuilder(new(FilterBuilder))
//  fb.AddFilter(db.Or(
//      db.Contains("_id", "1"),
//      db.Contains("group", "adm", "test")))
//  b, e := fb.Build()
//  if e != nil {
//      t.Errorf("Error %s", e.Error())
//  } else {
//      fmt.Printf("Result:\n%v\n", tk.JsonString(b))
//  }
// }

// /*func TestSelect(t *testing.T) {
//  c, e := prepareConnection()
//  if e != nil {
//      t.Errorf("Unable to connect %s \n", e.Error())
//      return
//  }
//  defer c.Close()

//  csr, e := c.NewQuery().Select("_id", "title").From("appusers").Order("-title").
//      Cursor(nil)
//  if e != nil {
//      t.Errorf("Cursor pre error: %s \n", e.Error())
//      return
//  }
//  if csr == nil {
//      t.Errorf("Cursor not initialized")
//      return
//  }
//  defer csr.Close()

//  ds, e := csr.Fetch(nil, 0, false)
//  if e != nil {
//      t.Errorf("Unable to fetch all: %s \n", e.Error())
//  } else {
//      fmt.Printf("Fetch all OK. Result: %d \n", len(ds.Data))
//  }

//  e = csr.ResetFetch()
//  if e != nil {
//      t.Errorf("Unable to reset fetch: %s \n", e.Error())
//  }

//  ds, e = csr.Fetch(nil, 3, false)
//  if e != nil {
//      t.Errorf("Unable to fetch N: %s \n", e.Error())
//  } else {
//      fmt.Printf("Fetch N OK. Result: %v \n",
//          ds.Data)
//  }
// }*/

// func TestSelectFilter(t *testing.T) {
//  t.Skip("Just Skip Test")
//  c, e := prepareConnection()
//  if e != nil {
//      t.Errorf("Unable to connect %s \n", e.Error())
//      return
//  }
//  defer c.Close()

//  csr, e := c.NewQuery().Select().
//      Where(db.Contains("fullname", "43")).
//      From("TestUsers").Cursor(nil)
//  if e != nil {
//      t.Errorf("Cursor pre error: %s \n", e.Error())
//      return
//  }
//  if csr == nil {
//      t.Errorf("Cursor not initialized")
//      return
//  }
//  defer csr.Close()

//  results := make([]map[string]interface{}, 0)
//  e = csr.Fetch(&results, 10, false)
//  if e != nil {
//      t.Errorf("Unable to fetch N1: %s \n", e.Error())
//  } else {
//      fmt.Printf("Fetch N1 OK. Result: %v \n", results)
//  }

//  csr, e = c.NewQuery().
//      Where(db.Contains("fullname", "43", "44")).
//      From("TestUsers").Cursor(nil)
//  if e != nil {
//      t.Errorf("Cursor pre error: %s \n", e.Error())
//      return
//  }
//  if csr == nil {
//      t.Errorf("Cursor not initialized")
//      return
//  }
//  defer csr.Close()

//  results = make([]map[string]interface{}, 0)
//  e = csr.Fetch(&results, 10, false)
//  if e != nil {
//      t.Errorf("Unable to fetch N1: %s \n", e.Error())
//  } else {
//      fmt.Printf("Fetch N2 OK. Result: %v \n", results)
//  }
// }

// /*func TestSelectAggregate(t *testing.T) {
//  c, e := prepareConnection()
//  if e != nil {
//      t.Errorf("Unable to connect %s \n", e.Error())
//      return
//  }
//  defer c.Close()

//  //fb := c.Fb()
//  csr, e := c.NewQuery().
//      Aggr(db.AggrSum, 1, "Sum").
//      Aggr(db.AggrMax, "$fullname", "Name").
//      From("ORMUsers").
//      Group("enable").
//      Cursor(nil)
//  if e != nil {
//      t.Errorf("Cursor pre error: %s \n", e.Error())
//      return
//  }
//  if csr == nil {
//      t.Errorf("Cursor not initialized")
//      return
//  }
//  defer csr.Close()

//  ds, e := csr.Fetch(nil, 0, false)
//  if e != nil {
//      t.Errorf("Unable to fetch: %s \n", e.Error())
//  } else {
//      fmt.Printf("Fetch OK. Result: %v \n",
//          tk.JsonString(ds.Data))

//  }
// }*/

// /*func TestSelectAggregateUsingCommand(t *testing.T) {
//  c, e := prepareConnection()
//  if e != nil {
//      t.Errorf("Unable to connect %s \n", e.Error())
//      return
//  }
//  defer c.Close()

//  //fb := c.Fb()
//  pipe := []tk.M{tk.M{}.Set("$group", tk.M{}.Set("_id", "$enable").Set("count", tk.M{}.Set("$sum", 1)))}
//  csr, e := c.NewQuery().
//      Command("pipe", pipe).
//      From("ORMUsers").
//      Cursor(nil)
//  if e != nil {
//      t.Errorf("Cursor pre error: %s \n", e.Error())
//      return
//  }
//  if csr == nil {
//      t.Errorf("Cursor not initialized")
//      return
//  }
//  defer csr.Close()

//  ds, e := csr.Fetch(nil, 0, false)
//  if e != nil {
//      t.Errorf("Unable to fetch: %s \n", e.Error())
//  } else {
//      fmt.Printf("Fetch OK. Result: %v \n",
//          tk.JsonString(ds.Data))
//  }
// }

// func TestProcedure(t *testing.T) {
//  c, _ := prepareConnection()
//  defer c.Close()

//  csr, e := c.NewQuery().Command("procedure", tk.M{}.Set("name", "spSomething").Set("parms", tk.M{}.Set("@name", "EACIIT"))).Cursor(nil)
//  if e != nil {
//      t.Error(e)
//      return
//  }
//  defer csr.Close()

//  ds, e := csr.Fetch(nil, 0, false)
//  if e != nil {
//      t.Errorf("Unable to fetch: %s \n", e.Error())
//  } else {
//      fmt.Printf("Fetch OK. Result: %v \n",
//          tk.JsonString(ds.Data))
//  }

// }*/

// // func TestCRUD(t *testing.T) {
// //   //t.Skip()
// //   c, e := prepareConnection()
// //   if e != nil {
// //       t.Errorf("Unable to connect %s \n", e.Error())
// //       return
// //   }
// //   defer c.Close()
// //   e = c.NewQuery().From("testtables").Delete().Exec(nil)
// //   if e != nil {
// //       t.Errorf("Unablet to clear table %s\n", e.Error())
// //       return
// //   }

// //   q := c.NewQuery().SetConfig("multiexec", true).From("testtables").Save()
// //   type user struct {
// //       Id    string `bson:"_id"`
// //       Title string
// //       Email string
// //   }
// //   for i := 1; i <= 10000; i++ {
// //       //go func(q db.IQuery, i int) {
// //       data := user{}
// //       data.Id = fmt.Sprintf("User-%d", i)
// //       data.Title = fmt.Sprintf("User-%d's name", i)
// //       data.Email = fmt.Sprintf("User-%d@myco.com", i)
// //       if i == 10 || i == 20 || i == 30 {
// //           data.Email = fmt.Sprintf("User-%d@myholding.com", i)
// //       }
// //       e = q.Exec(tk.M{
// //           "data": data,
// //       })
// //       if e != nil {
// //           t.Errorf("Unable to save: %s \n", e.Error())
// //       }
// //   }
// //   q.Close()

// //   data := user{}
// //   data.Id = fmt.Sprintf("User-15")
// //   data.Title = fmt.Sprintf("User Lima Belas")
// //   data.Email = fmt.Sprintf("user15@yahoo.com")
// //   e = c.NewQuery().From("testtables").Update().Exec(tk.M{"data": data})
// //   if e != nil {
// //       t.Errorf("Unable to update: %s \n", e.Error())
// //   }
// // }

// func TestGetObj(t *testing.T) {
//  t.Skip("Just Skip Test")
//  c, e := prepareConnection()
//  if e != nil {
//      t.Errorf("Unable to connect %s \n", e.Error())
//      return
//  }
//  defer c.Close()
//  //ObjTypeTable, ObjTypeView, ObjTypeProcedure, ObjTypeAll
//  tk.Printf("List Table : %v\n", c.ObjectNames(db.ObjTypeTable))
//  tk.Printf("List Procedure : %v\n", c.ObjectNames(db.ObjTypeProcedure))
//  tk.Printf("List All Object : %v\n", c.ObjectNames(""))
// }

// func TestInsertBulk(t *testing.T) {
//  // t.Skip("Just Skip Test")
//  c, e := prepareConnection()
//  if e != nil {
//      t.Errorf("Unable to connect %s \n", e.Error())
//      return
//  }
//  defer c.Close()
//  atkm := []tk.M{}

//  atkm = append(atkm, tk.M{}.Set("name", "alip").Set("company", "eaciit").Set("dtime", time.Now().UTC()).Set("Experience", float64(6.45)))
//  atkm = append(atkm, tk.M{}.Set("name", "eko").Set("company", "eaciit").Set("dtime", time.Now().UTC()).Set("Experience", int(6)))
//  atkm = append(atkm, tk.M{}.Set("name", "xxx").Set("company", "xxxxxx").Set("dtime", time.Now().UTC()).Set("Experience", 6.45))
//  _ = atkm
//  q := c.NewQuery().SetConfig("pooling", true).From("bulktest").Insert()
//  // e = q.Exec(tk.M{"data": tk.M{}.Set("name", "aaa").Set("city", "aaaaa")})
//  e = q.Exec(tk.M{"data": atkm})
//  if e != nil {
//      t.Errorf("Found %s \n", e.Error())
//      return
//  }

// }

// type dtests struct {
//  Name            string
//  City            string
//  YearsExperience int
//  StartWorking    time.Time
//  Married         bool
//  Benefit         float64
// }

// func TestInsertBulkStruct(t *testing.T) {
//  t.Skip("Just Skip Test")

//  c, e := prepareConnection()
//  if e != nil {
//      t.Errorf("Unable to connect %s \n", e.Error())
//      return
//  }
//  defer c.Close()
//  astruct := []dtests{}

//  astruct = append(astruct, dtests{Name: "Alip01", City: "Surabaya", YearsExperience: 5, StartWorking: time.Now().UTC(), Married: true, Benefit: 5000})
//  astruct = append(astruct, dtests{Name: "Alip02", City: "Ngawi", YearsExperience: 5, StartWorking: time.Now().UTC(), Married: true, Benefit: 5000})
//  astruct = append(astruct, dtests{Name: "Alip03", City: "Tuban", YearsExperience: 5, StartWorking: time.Now().UTC(), Married: true, Benefit: 5000})
//  _ = astruct
//  q := c.NewQuery().SetConfig("pooling", true).From("bulktest").Insert()
//  // e = q.Exec(tk.M{"data": tk.M{}.Set("name", "aaa").Set("city", "aaaaa")})
//  e = q.Exec(tk.M{"data": astruct})
//  if e != nil {
//      t.Errorf("Found %s \n", e.Error())
//      return
//  }

// }

// func TestInsertBulkStructPointer(t *testing.T) {
//  t.Skip("Just Skip Test")

//  c, e := prepareConnection()
//  if e != nil {
//      t.Errorf("Unable to connect %s \n", e.Error())
//      return
//  }
//  defer c.Close()
//  astruct := []*dtests{}

//  astruct = append(astruct, &dtests{Name: "Alip04", City: "Surabaya", YearsExperience: 5, StartWorking: time.Now().UTC(), Married: true, Benefit: 5000})
//  astruct = append(astruct, &dtests{Name: "Alip05", City: "Ngawi", YearsExperience: 5, StartWorking: time.Now().UTC(), Married: true, Benefit: 5000})
//  astruct = append(astruct, &dtests{Name: "Alip06", City: "Tuban", YearsExperience: 5, StartWorking: time.Now().UTC(), Married: true, Benefit: 5000})
//  _ = astruct
//  q := c.NewQuery().SetConfig("pooling", true).From("bulktest").Insert()
//  // e = q.Exec(tk.M{"data": tk.M{}.Set("name", "aaa").Set("city", "aaaaa")})
//  e = q.Exec(tk.M{"data": astruct})
//  if e != nil {
//      t.Errorf("Found %s \n", e.Error())
//      return
//  }

// }

// func TestInsertStructPointer(t *testing.T) {
//  t.Skip("Just Skip Test")

//  c, e := prepareConnection()
//  if e != nil {
//      t.Errorf("Unable to connect %s \n", e.Error())
//      return
//  }
//  defer c.Close()
//  q := c.NewQuery().SetConfig("pooling", true).From("bulktest").Insert()
//  // e = q.Exec(tk.M{"data": tk.M{}.Set("name", "aaa").Set("city", "aaaaa")})
//  e = q.Exec(tk.M{"data": &dtests{Name: "Alip07", City: "Surabaya", YearsExperience: 5, StartWorking: time.Now().UTC(), Married: true, Benefit: 5000}})
//  if e != nil {
//      t.Errorf("Found %s \n", e.Error())
//      return
//  }

// }

// func TestInsertStruct(t *testing.T) {
//  t.Skip("Just Skip Test")

//  c, e := prepareConnection()
//  if e != nil {
//      t.Errorf("Unable to connect %s \n", e.Error())
//      return
//  }
//  defer c.Close()
//  q := c.NewQuery().SetConfig("pooling", true).From("bulktest").Insert()
//  // e = q.Exec(tk.M{"data": tk.M{}.Set("name", "aaa").Set("city", "aaaaa")})
//  e = q.Exec(tk.M{"data": dtests{Name: "Alip08", City: "Surabaya", YearsExperience: 5, StartWorking: time.Now().UTC(), Married: true, Benefit: 5000}})
//  if e != nil {
//      t.Errorf("Found %s \n", e.Error())
//      return
//  }

// }
