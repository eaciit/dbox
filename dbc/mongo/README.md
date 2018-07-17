# Dbox Mongo Driver

### Testing

First of all, start new mongo server, then adjust the connection credentials on the `mgo_connection_test.go` to reflect your mongo server.

Then run `./coverage_test.sh` to start the testing (+ coverage test).

### Connection Leak Tracing

Please refer [trace.md](https://github.com/eaciit/dbox/blob/dev/dbc/mongo/Trace.md).
