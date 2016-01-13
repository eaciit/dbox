package mongo

import (
	"github.com/eaciit/dbox"
	"gopkg.in/mgo.v2"

	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"time"
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
	c.SetInfo(ci)
	c.SetFb(dbox.NewFilterBuilder(new(FilterBuilder)))
	return c, nil
}

func (c *Connection) Connect() error {
	info := new(mgo.DialInfo)
	ci := c.Info()
	if ci == nil {
		return errorlib.Error(packageName, modConnection, "Connect", "ConnectionInfo is not initialized")
	}
	if ci.UserName != "" {
		info.Username = ci.UserName
		info.Password = ci.Password
		info.Source = "admin"
	}
	info.Addrs = []string{ci.Host}
	info.Database = ci.Database

	if ci.Settings == nil {
		ci.Settings = toolkit.M{}
	}

	poollimit := ci.Settings.GetInt("poollimit")
	if poollimit > 0 {
		info.PoolLimit = poollimit
	}

	timeout := ci.Settings.GetInt("timeout")
	if timeout > 0 {
		info.Timeout = time.Duration(timeout) * time.Second
	}

	//sess, e := mgo.Dial(info.Addrs[0])
	sess, e := mgo.DialWithInfo(info)
	if e != nil {
		return errorlib.Error(packageName, modConnection,
			"Connect", e.Error()+" "+ci.UserName+"@"+ci.Host+"/"+ci.Database)
	}
	sess.SetMode(mgo.Monotonic, true)
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
	if c.session != nil {
		c.session.Close()
	}
}
