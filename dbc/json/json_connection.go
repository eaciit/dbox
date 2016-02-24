package json

import (
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
	filePath, sData string
	openFile        *os.File
	isNewSave       bool
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
	c.Close()

	ci := c.Info()

	if ci == nil || ci.Host == "" {
		return errorlib.Error(packageName, modConnection, "Connect", "No connection info")
	}

	_, e := os.Stat(ci.Host)
	if ci.Settings != nil {
		if ci.Settings["newfile"] == true {
			if os.IsNotExist(e) {
				create, _ := os.Create(ci.Host)
				create.Close()
			}
		} else {
			if os.IsNotExist(e) {
				return errorlib.Error(packageName, modConnection, "Connect", "Create new file is false")
			}
		}
	} else if os.IsNotExist(e) {
		return errorlib.Error(packageName, modConnection, "Connect", "No json file found")
	}

	c.filePath = ci.Host

	return nil
}

func (c *Connection) NewQuery() dbox.IQuery {
	q := new(Query)
	q.SetConnection(c)
	q.SetThis(q)
	return q
}

func (c *Connection) OpenSession() error {
	c.Close()

	t, e := os.OpenFile(c.filePath, os.O_RDWR, 0)
	if e != nil {
		return errorlib.Error(packageName, modConnection, "OpenFile", e.Error())
	}
	c.openFile = t
	c.sData = ""
	return nil
}

func (c *Connection) Close() {
	if c.openFile != nil {
		c.openFile.Close()
	}
}
