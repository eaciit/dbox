package mysql

import (
	"github.com/eaciit/dbox" 
	"fmt"
	// err "github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit" 
	_ "github.com/go-sql-driver/mysql"
	"github.com/eaciit/dbox/dbc/rdbms" 
	// "database/sql"	
)

const (
	packageName   = "eaciit.dbox.dbc.mysql"
	modConnection = "Connection"
)

type Connection struct {
	rdbms.Connection  
}


func init() {
	dbox.RegisterConnector("mysql", NewConnection)
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
	ConnectionString:=username+":"+pass+"@tcp("+host+")/"+db //user:password@tcp(127.0.0.1:3306)/hello"	 
	// sqlcon, e := sql.Open("mysql", ConnectionString)
	// if e != nil {
	// 	return err.Error(packageName, modConnection, "Connect", e.Error())
	// }
	// c.Sql = *sqlcon
	err :=c.RdbmsConnect("mysql",ConnectionString)
	if(err != nil){
		fmt.Println(err)
	}
	return nil
}
 
