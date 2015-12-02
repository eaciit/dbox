package json

import (
	// "fmt"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"os"
)

const (
	packageName   = "eaciit.dbox.dbc.json"
	modConnection = "Connection"
)

type Connection struct {
	dbox.Connection
	session *os.File
}

func init() {
	dbox.RegisterConnector("json", NewConnection)
}

func NewConnection(ci *dbox.ConnectionInfo) (dbox.IConnection, error) {
	if ci.Settings == nil {
		ci.Settings = toolkit.M{}
	}

	c := new(Connection)
	c.SetInfo(ci)
	c.SetFb(dbox.NewFilterBuilder(new(FilterBuilder)))

	return c, nil
}

func (c *Connection) Connect() error {
	ci := c.Info()

	if ci == nil {
		return errorlib.Error(packageName, modConnection, "Connect", "No connection info")
	} else if ci.Host == "" {
		return errorlib.Error(packageName, modConnection, "Connect", "Read file is not initialized")
	}

	sess, _ := os.Open(ci.Host)
	c.session = sess
	return nil
}

func (c *Connection) NewQuery() dbox.IQuery {
	q := new(Query)
	q.SetConnection(c)
	q.SetThis(q)
	return q
}

func (c *Connection) Close() {
	c.session.Close()
}
