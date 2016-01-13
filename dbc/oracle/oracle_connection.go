package oracle

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-oci8"
	"os"
	"strings"	
)

const (
	packageName   = "eaciit.dbox.dbc.oracle"
	modConnection = "Connection"
)

type Connection struct {
	rdbms.Connection  
}


func init() {
	dbox.RegisterConnector("oracle", NewConnection)
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
	ConnectionString:= username+"/"+pass+"@"+host
	err :=c.RdbmsConnect("oci8",ConnectionString)
	if(err != nil){
		fmt.Println(err)
	}
	return nil
}
 
