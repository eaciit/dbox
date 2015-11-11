package mongo

import (
	"github.com/eaciit/dbox"
	"gopkg.in/mgo.v2"

	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	_ "gopkg.in/mgo.v2/bson"
)

const (
	packageName   = "eaciit.dbox.dbc.mongo"
	modConnection = "Connection"
)

type Connection struct {
	dbox.Connection

	session *mgo.Session
}

func init() {
	dbox.RegisterConnector("mongo", NewConnection)
}

func NewConnection(ci *dbox.ConnectionInfo) (dbox.IConnection, error) {
	if ci.Settings == nil {
		ci.Settings = toolkit.M{}
	}
	c := new(Connection)
	c.Info = ci
	return c, nil
}

func (c *Connection) Connect() error {
	info := new(mgo.DialInfo)
	ci := c.Info
	if ci.UserName != "" {
		info.Username = ci.UserName
		info.Password = ci.Password
	}
	info.Addrs = []string{ci.Host}
	info.Database = ci.Database
	info.Source = "admin"

	if ci.Settings != nil {
		ci.Settings = toolkit.M{}
	}

	if ci.Settings.Get("poollimit", 0).(int) > 0 {
		info.PoolLimit = 100
	}

	sess, e := mgo.DialWithInfo(info)
	if e != nil {
		return errorlib.Error(packageName, modConnection,
			"Connect", e.Error())
	}
	sess.SetMode(mgo.Monotonic, true)
	c.session = sess
	return nil
}

func (c *Connection) Close() {
	if c.session != nil {
		c.session.Close()
	}
}
