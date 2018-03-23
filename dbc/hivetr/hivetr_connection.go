package hivetr

import (
	"strconv"
	"strings"

	err "github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"github.com/kharism/dbox"
	"github.com/kharism/dbox/dbc/rdbms"
	"github.com/kharism/gohive"
)

const (
	packageName   = "eaciit.dbox.dbc.hivetr"
	modConnection = "Connection"
)

type Connection struct {
	rdbms.Connection
	Conn *gohive.TSaslClientTransport
}

func init() {
	dbox.RegisterConnector("hivetr", NewConnection)
}
func NewConnection(ci *dbox.ConnectionInfo) (dbox.IConnection, error) {
	if ci.Settings == nil {
		ci.Settings = toolkit.M{}
	}
	c := new(Connection)
	c.SetInfo(ci)
	hostInfo := strings.Split(ci.Host, ":")
	port := 10000
	if len(hostInfo) == 2 {
		port2, e := strconv.Atoi(hostInfo[1])
		if e != nil {
			port = 10000
		} else {
			port = port2
		}
	}
	Conn, e := gohive.NewTSaslTransport(hostInfo[0], port, ci.UserName, ci.Password, gohive.DefaultOptions)
	if e != nil {
		return nil, err.Error(packageName, "Connection", "NewConnection", e.Error())
	}
	c.Conn = Conn
	return c, nil
}
func (c *Connection) Connect() error {
	e := c.Conn.Open()
	if e != nil {
		return err.Error(packageName, "Connection", "Open Connection", e.Error())
	}
	switchDb, e := c.Conn.Query("use " + c.Info().Database)
	if e != nil {
		return err.Error(packageName, "Connection", "Open Connection", e.Error())
	}
	return switchDb.Close()
}
func (c *Connection) Close() {
	c.Conn.Close()
}
func (c *Connection) NewQuery() dbox.IQuery {
	q := &Query{}
	q.SetThis(q)
	q.SetConnection(c)
	return q
}
func (c *Connection) NewRawQuery(query string) dbox.IQuery {
	q := &Query{}
	q.SetThis(q)
	q.RawQuery = query
	q.SetConnection(c)
	return q
}
