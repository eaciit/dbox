package rdbms

import (
	"github.com/eaciit/dbox"  
	err "github.com/eaciit/errorlib"
	// "github.com/eaciit/toolkit" 
	"database/sql"
)

const (
	packageName   = "eaciit.dbox.dbc.rdbms"
	modConnection = "Connection"
)

type Connection struct {
	dbox.Connection 
	Sql              sql.DB 
	Drivername       string
} 

func (c *Connection) RdbmsConnect(drivername string,stringConnection string) error {
	sqlcon, e := sql.Open(drivername, stringConnection)
	if e != nil {
		return err.Error(packageName, modConnection, "Connect", e.Error())
	} 
	c.Sql = *sqlcon  
	c.Drivername = drivername  
	return nil
}

func (c *Connection) NewQuery() dbox.IQuery {
	q := new(Query)
	q.SetConnection(c)
	q.SetThis(q)	 
	return q
}

func (c *Connection) GetDriver() string {
	return c.Drivername
}

func (c *Connection) Close() {
	c.Sql.Close()
}


