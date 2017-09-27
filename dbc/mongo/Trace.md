
# Database Connection Leak
Be careful when building your mongo connection. Each cursor should be
closed. If forgot to close, the connection will linger and live until
the program dead. This will take up server resources.
```
cur, err := conn.NewQuery().
    From("Student").
    Cursor(nil)
if err != nil {
    return
}

// THIS IS IMPORTANT!!!
// DO NOT FORGET TO CLOSE CURSOR
defer cur.Close()
```

## Detecting
Connection leak can be found when connection count on database is keep
increasing on alarming rate. You can count database connection using
shell command.

Assuming mongo database port is 27017.

 - On Windows Cmd (must be run as administrator)
 ```
 netstat -nop tcp | findstr /R /C:":27017 *ESTABLISHED"
 ```

 - On Linux
 ```
 sudo netstat -ptn | grep ":27017 *ESTABLISHED"
 ```

Run above shell command once, try to playing with your application. Open
many pages, stop a few seconds, and then come back to shell and run the 
command again. Number of established connection should not increase 
dramatically each time you open any page.

## Tracing (beta)
Mongo driver comes with database connection tracing. To enable it, you
must call StartTrace() before doing anything with database. And then
call PrintTrace() to print the source of leaking connection.

```
import (
    "github.com/eaciit/dbox/dbc/mongo"
)

func main() {
    // Put StartTrace before connecting to database
    mongo.StartTrace()

    // Your knot code
    ...
    knot.StartAppWithFn(app, knotListen, routes)
    ...

    // Put PrintTrace in the end of main function
    mongo.PrintTrace()
}
```

Rebuild your application, and run it again. After playing with app a few
times, you can stop your application with opening /stop url. If your app
port is 1234 then open url http://127.0.0.1:1234/stop in your browser.
This will stop knot server and run PrintTrace().

Below is example of PrintTrace() on leaked program,
```
::Connection Trace Result: 10 probably leaked
::Connection 1
github.com/eaciit/dbox/dbc/mongo.(*Query).Cursor(0xc042165400, 0x0, 0x2, 0x2, 0xed6d20, 0xc042165400)
  C:/Users/Airlangga/Projects/project/src/github.com/eaciit/dbox/dbc/mongo/mgo_query.go:226 +0x20d6
eaciit/project/webapps/controllers.AttachCustomerProfile(0xed4b60, 0xc042241a60, 0xc04243f020)
  C:/Users/Airlangga/Projects/project/src/eaciit/project/webapps/controllers/databrowser.go:367 +0x367
eaciit/project/webapps/controllers.(*DataBrowserController).GetCombinedData(0xc042004040, 0xc042165720, 0x0, 0x0)
  C:/Users/Airlangga/Projects/project/src/eaciit/project/webapps/controllers/databrowser.go:498 +0x8ce
reflect.callMethod(0xc0421e6b40, 0xc0423f9bb8)
  C:/Users/Airlangga/Applications/go/src/reflect/value.go:640 +0x191
reflect.methodValueCall(0xc042165720, 0x0, 0x0, 0x0, 0xc04206a600, 0x1c, 0xecc220, 0xc0421f2ba0, 0xc0421f2b80, 0x1c, ...)
  C:/Users/Airlangga/Applications/go/src/reflect/asm_amd64.s:29 +0x47
github.com/eaciit/knot/knot%2ev1.(*Server).RouteWithConfig.func1(0xed0e20, 0xc042304ee0, 0xc04215db00)
  c:/Users/Airlangga/Projects/project/src/github.com/eaciit/knot/knot.v1/server.go:212 +0x21d
net/http.HandlerFunc.ServeHTTP(0xc0421f2ba0, 0xed0e20, 0xc042304ee0, 0xc04215db00)
  C:/Users/Airlangga/Applications/go/src/net/http/server.go:1942 +0x4b
net/http.(*ServeMux).ServeHTTP(0xc04200f5c0, 0xed0e20, 0xc042304ee0, 0xc04215db00)
  C:/Users/Airlangga/Applications/go/src/net/http/server.go:2238 +0x137
net/http.serverHandler.ServeHTTP(0xc042296000, 0xed0e20, 0xc042304ee0, 0xc04215db00)
  C:/Users/Airlangga/Applications/go/src/net/http/server.go:2568 +0x99
net/http.(*conn).serve(0xc0420432c0, 0xed1820, 0xc042156680)
  C:/Users/Airlangga/Applications/go/src/net/http/server.go:1825 +0x619
created by net/http.(*Server).Serve
  C:/Users/Airlangga/Applications/go/src/net/http/server.go:2668 +0x2d5
```

Trace will print some possibility of connection leak on your console.
From above output, we found out connection leak may come from
go source file databrowser.go:367.

Happy Debuging!