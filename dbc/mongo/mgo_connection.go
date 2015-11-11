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

func NewConnection(host, database, username,
	password string, settings toolkit.M) *Connection {
	if settings == nil {
		settings = toolkit.M{}
	}
	c := new(Connection)
	c.Host = host
	c.Database = database
	c.UserName = username
	c.Password = password
	c.Settings = settings
	return c
}

func (c *Connection) Connect() error {
	info := new(mgo.DialInfo)
	if c.UserName != "" {
		info.Username = c.UserName
		info.Password = c.Password
	}
	info.Addrs = []string{c.Host}
	info.Database = c.Database
	info.Source = "admin"

	if c.Settings != nil {
		c.Settings = toolkit.M{}
	}

	if c.Settings.Get("poollimit", 0).(int) > 0 {
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
