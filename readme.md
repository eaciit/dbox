
# DBOX

A multi database abstraction.

## Database Support

We have driver support for:
 * CSV
 * Hive
 * JDBC
 * JSON
 * Mongo
 * MsSQL
 * MySQL
 * ODBC
 * Oracle
 * Postgres
 * XLSX

## Quick Start

### Important Note

This quick start assuming mongodb as a driver. If you are using other driver,
make sure you do some adjustment to your code. Especially insert and update
section.

### Import Database driver

First of all, we need to specify which driver we need. Just like golang sql
driver, we just need to import with blank identifier. Dbox driver will
initialize itself using import side-effect.

```golang
import {
    "github.com/eaciit/dbox"
    _ "github.com/eaciit/dbox/dbc/mongo"
}
```

Now our driver is initialized.

### Create Connection

Next, we create our connection info struct. In this example we want to connect to MongoDB Database.

```golang
    ci := dbox.ConnectionInfo {
        "127.0.0.1",
        "dbname",
        "user",
        "pass",
        nil
    }
    conn, e := dbox.NewConnection("mongo", ci)
    err := conn.Connect()
    if err != nil {
        panic("Connect Failed"); // Change with your error handling
    }

    // Do something with database using conn
```

### Select Query

With conn in our hand, now we can manipulate database at will. For example we
want to pull data from collection "Users" that have field "_id" = "0123456789".

```golang
    q, err := conn.NewQuery().
        From("Users").
        Where(dbox.Eq("_id", "0123456789")).
        Cursor(nil)
    if err != nil {
        panic("Query Failed");
    }

    // You need to close query
    // Protip: Make sure you check for error before defering Close()
    defer q.Close()
```

Now q contains our query cursor. We can fetch one by one or all of them at once.

```golang
    // This map is only for quick example
    // It's better to use some struct to create stronger type check
    users := []map[string]interface{}{}
    q.Fetch(&users, 0, false)
```

### Insert

Pulling out data is not gonna usefull without saving data. We will now learn how
to insert data into our database.

First, make sure you have working conn. And then we create new insert query:

```golang
    q := conn.NewQuery().From("Users").SetConfig("multiexec", true).Save()
    // Make sure q is closed when exiting function
    defer q.Close()

    // This map is only for quick example
    // It's better to use some struct to create stronger type check
    user := map[string]interface{}

    // To enforce new data, you need to put some uniuserue identifier
    // Neglected to do this may replace all your data with this data
    // Instead of NewObjectId(), you can create your own primary key combination
    user["_id"] = bson.NewObjectId()

    // Put all other field data
    user["name"] = "EACIIT"

    // Wrap data query
    newdata := map[string]interface{}{"data": user}

    // Execute insertion
    err = q.Exec(newdata)
    if err != nil {
        panic("Query Failed");
    }
```

### Update

Update code is almost as same as insert. But instead of generating new Object
Id, use the old Object Id instead.

```golang
    // Get user object, please refer to Select Query section
    users := []map[string]interface{}{}
    q.Fetch(&users, 0, false)

    if len(users) == 0 {
        panic("User not found");
    }
    // Updating data
    newUser := map[string]interface{}
    // Take _id from the first user row
    // The other way is direcly manipulate users[0] instead of creating new var
    // Just make sure _id is same
    newUser["_id"] = users[0]["_id"]
    newUser["name"] = "New User"

    // Create new cursor
    qInsert := conn.NewQuery().From("Users").SetConfig("multiexec", true).Save()
    // Make sure q is closed when exiting function
    defer qInsert.Close()

    // Wrap data query
    newdata := map[string]interface{}{"data": newUser}

    // Execute update
    err = qInsert.Exec(newdata)
    if err != nil {
        panic("Query Failed");
    }
```

When mongodb encounter insertion with same _id, it will do replacement instead.
