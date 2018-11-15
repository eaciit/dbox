# Pooling 

Use `dbox.NewDbPooling()` to create a db connection pool. Fill the 1st parameter with a number represent the max allowed connection, and the 2nd parameter with closure that return `(dbox.IConnection, error)` data.

The database connection initialization will happen inside the closure.

```go
func DBConnection() (*dbox.DbPooling, error) {
	connectionPool := dbox.NewDbPooling(5, func() (dbox.IConnection, error) {
		connInfo := &dbox.ConnectionInfo{
			Host:     "localhost:27123",
			Database: "test",
			UserName: "",
			Password: "",
		}

		conn, err := dbox.NewConnection("mongo", connInfo)
		if err != nil {
			return nil, err
		}

		err = conn.Connect()
		if err != nil {
			return nil, err
		}

		return conn, nil
	})
	connectionPool.Timeout = 30 * time.Second

	return connectionPool, nil
}

func main() {
	pool, err := DBConnection()
	defer pool.Close()
	if err != nil {
		log.Fatal(err.Error())
		return
	}

	// ...
}
```

On code above, max allowed connection is set to `5`, meaning that only 5 concurrent connection are allowed (remember, **CONCURRENT**, meaning the processes are happening in near-same time!). 

If another one connection needed, meanwhile the current active connection are met the maximum number allowed, then it'll go into queue. If, before the `connectionPool.Timeout` there is one free connection, then it'll be immediately used.

Use the `.Get()` method from pool object to get one what-so-called pool item (contains the db connection object). Better to immediately defer `.Release()` the pool item after being used.

```go
poolItem, err := pool.Get()
defer poolItem.Release()
if err != nil {
	log.Fatal(err.Error())
	return
}

// ...
```

If some database process is required inside a loop, don't use defer release, instead release it explicitly after the process done. Because defer will make the statement executed at the end of block function. And loop is not a block of function.

```go
for _, each := range data {
	poolItem, err := pool.Get()
	
	if err != nil {
		log.Error(err.Error())
		poolItem.Release()
		continue
	}

	// ...
	poolItem.Release()
}
```

Or, just wrap it with anonymous function.

```go
for _, each := range data {
	func () {
		poolItem, err := pool.Get()
		defer poolItem.Release()
		
		if err != nil {
			log.Error(err.Error())
			return
		}

		// ...
	}()
}
```

If the process inside the loop will be executed concurrently, don't forget to pass the loop data. No need to pass the `pool` object since it's already a pointer.

```go
for _, each := range data {
	go func (each Data) {
		poolItem, err := pool.Get()
		defer poolItem.Release()
		
		if err != nil {
			log.Error(err.Error())
			return
		}

		// ...
	}(each)
}
```
