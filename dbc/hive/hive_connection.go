package hive

import (
	"github.com/eaciit/cast"
	"github.com/eaciit/dbox"
	"github.com/eaciit/dbox/dbc/rdbms"
	err "github.com/eaciit/errorlib"
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
	path := cast.ToString(ci.Settings["path"])
	delimiter := cast.ToString(ci.Settings["delimiter"])
	ConnectionString := host + "," + db + "," + username + "," + pass + "," + path + "," + delimiter

	e := c.RdbmsConnect("hive", ConnectionString)
	if e != nil {
		return err.Error(packageName, modConnection, "Connect", e.Error())
	}
	return nil
}

func (c *Connection) Close() {
	c.Connection.Close()
}
