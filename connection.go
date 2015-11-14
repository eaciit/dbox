package dbox

import (
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
)

type IConnection interface {
	Connect() error
	Close()

	Info() *ConnectionInfo
	SetInfo(*ConnectionInfo)

	NewQuery() IQuery
	Fb() IFilterBuilder
}

type FnNewConnection func(*ConnectionInfo) (IConnection, error)

var connectors map[string]FnNewConnection

func RegisterConnector(connector string, fn FnNewConnection) {
	if connectors == nil {
		connectors = map[string]FnNewConnection{}
	}
	connectors[connector] = fn
}

func NewConnection(connector string, ci *ConnectionInfo) (IConnection, error) {
	if connectors == nil {
		return nil, errorlib.Error(packageName, "", "NewConnection", "Invalid connector")
	}

	fn, found := connectors[connector]
	if found == false {
		return nil, errorlib.Error(packageName, "", "NewConnection", "Invalid connector")
	}
	return fn(ci)
}

type ConnectionInfo struct {
	Host     string
	Database string
	UserName string
	Password string

	Settings toolkit.M
}

type Connection struct {
	info *ConnectionInfo
	fb   IFilterBuilder
}

func (c *Connection) Connect() error {
	return errorlib.Error(packageName, modConnection,
		"Connect", errorlib.NotYetImplemented)
}

func (c *Connection) Info() *ConnectionInfo {
	return c.info
}

func (c *Connection) SetInfo(i *ConnectionInfo) {
	c.info = i
}

func (c *Connection) Fb() IFilterBuilder {
	if c.fb == nil {
		c.fb = new(FilterBuilder)
	}
	return c.fb
}

func (c *Connection) Close() {
}

func (c *Connection) NewQuery() IQuery {
	q := new(Query)
	return q
}
