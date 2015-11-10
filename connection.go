package dbox

import (
	"github.com/eaciit/toolkit"

	"github.com/eaciit/errorlib"
)

type IConnection interface {
	Connect() error
	Close()
}

type Connection struct {
	Host     string
	UserName string
	Password string
	Database string

	Settings toolkit.M
}

func (c *Connection) Connect() error {
	return errorlib.Error(packageName, modConnection,
		"Connect", errorlib.NotYetImplemented)
}

func (c *Connection) Close() {
	return errorlib.Error(packageName, modConnection,
		"Connect", errorlib.NotYetImplemented)
}
