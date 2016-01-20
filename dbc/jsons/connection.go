package jsons

import (
	"github.com/eaciit/dbox"
	err "github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"os"
)

func init() {
	dbox.RegisterConnector("jsons", NewConnection)
}

func NewConnection(ci *dbox.ConnectionInfo) (dbox.IConnection, error) {
	if ci.Settings == nil {
		ci.Settings = toolkit.M{}
	}
	c := new(Connection)
	c.folder = ci.Host
	c.SetInfo(ci)
	c.SetFb(dbox.NewFilterBuilder(new(FilterBuilder)))
	return c, nil
}

type Connection struct {
	dbox.Connection

	folder      string
	defautQuery *Query
}

func (c *Connection) Connect() error {
	c.folder = c.Info().Host
	if c.folder == "" {
		return err.Error(packageName, modConnection, "Connect", "folder path is empty")
	}

	_, e := os.Stat(c.folder)
	if e != nil {
		return err.Error(packageName, modConnection, "Connect",
			e.Error())
	}

	return nil
}

func (c *Connection) NewQuery() dbox.IQuery {
	pooling := c.Info().Settings.Get("pooling", false).(bool)

	if pooling && c.defautQuery != nil {
		return c.defautQuery
	} else {
		q := new(Query)
		q.SetConnection(c)
		q.SetThis(q)
		/*
			if pooling {
				c.defautQuery = q
			}
		*/
		return q
	}
	return nil
}

func (c *Connection) Close() {
}
