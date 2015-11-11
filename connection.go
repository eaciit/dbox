package dbox

import (
	"github.com/eaciit/toolkit"

	"github.com/eaciit/errorlib"
)

type IConnection interface {
	Connect() error
	Close()

	NewQuery() IQuery
	Fb() IFilterBuilder
}

type Connection struct {
	Host     string
	UserName string
	Password string
	Database string

	Settings toolkit.M

	fb IFilterBuilder
}

func (c *Connection) Connect() error {
	return errorlib.Error(packageName, modConnection,
		"Connect", errorlib.NotYetImplemented)
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
	return nil
}
