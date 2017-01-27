
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

### Query

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
```

Now q contains our query cursor. We can fetch one by one or all of them at once.

```golang
    users := []map[string]interface{}{}
    q.Fetch(&users, 0, false)
```
