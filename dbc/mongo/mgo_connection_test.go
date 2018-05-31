package mongo

import (
	"fmt"
	db "github.com/eaciit/dbox"
	tk "github.com/eaciit/toolkit"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

// store mongo connection info
var connectionInfo = &db.ConnectionInfo{
	Host:     "localhost:27123",
	Database: "dbtest",
	UserName: "",
	Password: "",
	Settings: tk.M{"timeout": 3},
}

// connection object
var connectionGlobal db.IConnection

func TestInitConnection(t *testing.T) {
	// create new connection
	var err error
	connectionGlobal, err = db.NewConnection("mongo", connectionInfo)
	assert.NoError(t, err)

	// try to connect as per connectionInfo
	err = connectionGlobal.Connect()
	assert.NoError(t, err)

	// connectionInfo should be equal with connection info data from connection object
	assert.Equal(t, connectionInfo, connectionGlobal.Info())

	// when mgo session is created, doing Ping() should returns nil
	pingResult := connectionGlobal.(*Connection).session.Ping()
	assert.NoError(t, pingResult)
}

func TestClosedConnection(t *testing.T) {
	// now we try to close the mgo session
	connectionGlobal.Close()

	// then Ping() it. error must be raised in this blocks since the session is already closed
	// panic will occur, that's why we encapsulate this process into anynimouse function, coz it's easier to recover from it
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("%v", r)
			}
		}()

		connectionGlobal.(*Connection).session.Ping()
	}()

	// the error should be contains some string "Session already closed"
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Session already closed")
}

func TestReInitConnection(t *testing.T) {
	// since session is closed, now try to create new session again
	var err error
	connectionGlobal, err = db.NewConnection("mongo", connectionInfo)
	assert.NoError(t, err)

	// try to connect as per connectionInfo
	err = connectionGlobal.Connect()
	assert.NoError(t, err)

	// when mgo session is created, doing Ping() should returns nil
	pingResult := connectionGlobal.(*Connection).session.Ping()
	assert.NoError(t, pingResult)
}

func TestInitConnectionIn200GoRoutine(t *testing.T) {
	wg := new(sync.WaitGroup)
	proposedTotalConnections := 200

	wg.Add(proposedTotalConnections)

	for i := 0; i < proposedTotalConnections; i++ {
		go func() {

			var recoverErr error
			func(recoverErr *error) {
				defer func() {
					if r := recover(); r != nil {
						*recoverErr = fmt.Errorf("%v", r)
					}
				}()

				connection, err := db.NewConnection("mongo", connectionInfo)
				if err != nil {
					*recoverErr = err
					return
				}

				err = connection.Connect()
				if err != nil {
					*recoverErr = err
					return
				}

				err = connection.(*Connection).session.Ping()
				if err != nil {
					*recoverErr = err
					return
				}
			}(&recoverErr)

			assert.NoError(t, recoverErr)
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestInitConnectionWithNilConnectionInfo(t *testing.T) {
	t.Skip("TestInitConnectionWithNilConnectionInfo is still error, need some fix")

	_, err := db.NewConnection("mongo", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ConnectionInfo is not initialized")
}

func TestInitConnectionWithInvalidDriverName(t *testing.T) {
	_, err := db.NewConnection("mondo", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid connector")
}

func TestInitConnectionOnInitializedConnection(t *testing.T) {
	t.Skip("TestInitConnectionOnInitializedConnection is still error, need some fix")

	connection, err := db.NewConnection("mongo", connectionInfo)
	assert.NoError(t, err)

	err = connection.Connect()
	assert.NoError(t, err)

	err = connection.Connect()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Session is started already")
}
