package mssql

import (
	"github.com/eaciit/dbox" 
	"fmt"	
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/eaciit/toolkit" 
	_ "github.com/go-sql-driver/mysql"
	"github.com/eaciit/dbox/dbc/rdbms" 
	// "database/sql"	
)

const (
	packageName   = "eaciit.dbox.dbc.mssql"
	modConnection = "Connection"
)

type Connection struct {
	rdbms.Connection  
}


func init() {
	dbox.RegisterConnector("mssql", NewConnection)
}
 func NewConnection(ci *dbox.ConnectionInfo) (dbox.IConnection, error) {
	if ci.Settings == nil {
		ci.Settings = toolkit.M{}
	} 
	c := new(Connection)  
	c.SetInfo(ci)
	c.SetFb(dbox.NewFilterBuilder(new(rdbms.FilterBuilder))) 
	return c, nil
}

func (c *Connection) Connect() error {
	ci       := c.Info()
    host     := ci.Host
	db       := ci.Database 
	username := ci.UserName
	pass     := ci.Password
 	ConnectionString := fmt.Sprintf("server=%s;database=%s;user id=%s;password=%s", host, db, username, pass)
	err :=c.RdbmsConnect("mssql",ConnectionString)
	if(err != nil){
		fmt.Println(err)
	}
	return nil
}
 
