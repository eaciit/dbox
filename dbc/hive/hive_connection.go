package hive

import (
	"fmt"
	"github.com/eaciit/dbox"
	// err "github.com/eaciit/errorlib"
	"github.com/eaciit/dbox/dbc/rdbms"
	"github.com/eaciit/toolkit"
	// "database/sql"
)

const (
	packageName   = "eaciit.dbox.dbc.hive"
	modConnection = "Connection"
)

type Connection struct {
	rdbms.Connection
}

func init() {
	dbox.RegisterConnector("hive", NewConnection)
}
func NewConnection(ci *dbox.ConnectionInfo) (dbox.IConnection, error) {
	if ci.Settings == nil {
		ci.Settings = toolkit.M{}
	}
	c := new(Connection)
	c.SetInfo(ci)
	c.SetFb(dbox.NewFilterBuilder(new(rdbms.FilterBuilder)))
	return c, nil
}

func (c *Connection) Connect() error {
	ci := c.Info()
	host := ci.Host
	db := ci.Database
	username := ci.UserName
	pass := ci.Password
	// ConnectionString := username + ":" + pass + "@tcp(" + host + ")/" + db //user:password@tcp(127.0.0.1:3306)/hello"
	ConnectionString := host + "," + db + "," + username + "," + pass + "," + ""
	// h = HiveConfig("192.168.0.223:10000", "default", "developer", "b1gD@T@")

	err := c.RdbmsConnect("hive", ConnectionString)
	if err != nil {
		fmt.Println(err)
	}
	return nil
}
