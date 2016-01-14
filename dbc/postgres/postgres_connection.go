package postgres

import (
	"github.com/eaciit/dbox" 
	"fmt" 
	"github.com/eaciit/toolkit" 
	_ "github.com/lib/pq"
	"github.com/eaciit/dbox/dbc/rdbms"  
)

const (
	packageName   = "eaciit.dbox.dbc.postgres"
	modConnection = "Connection"
)

type Connection struct {
	rdbms.Connection  
}


func init() {
	dbox.RegisterConnector("postgres", NewConnection)
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
    // host     := ci.Host
	db       := ci.Database 
	username := ci.UserName
	pass     := ci.Password
	ConnectionString := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable",
        username, pass, db)
	 
	err :=c.RdbmsConnect("postgres",ConnectionString)
	if(err != nil){
		fmt.Println(err)
	}
	return nil
}
 
