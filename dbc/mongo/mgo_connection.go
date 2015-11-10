package mongo

import (
	"github.com/eaciit/dbox"
	"github.com/eaciit/dbox/dbc/mongo"
	"gopkg.in/mgo.v2"

	"github.com/eaciit/toolkit"
	_ "gopkg.in/mgo.v2/bson"
)

type Connection struct {
	dbox.Connection

	session *mgo.Session
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

	if c.Settings.Has("poollimit", 0) > 0 {
		info.PoolLimit = 100
	}

	sess, e := mgo.DialWithInfo(info)
	if e != nil {
		return err.Error(packageName, modConnection, "Connect", e.Error())
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
