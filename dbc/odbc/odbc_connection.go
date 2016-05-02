package odbc

import (
	// "database/sql"
	"github.com/eaciit/dbox"
	"github.com/eaciit/dbox/dbc/rdbms"
	err "github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"odbc"
	// _ "go-odbc/driver"
)

const (
	packageName   = "eaciit.dbox.dbc.odbc"
	modConnection = "Connection"
)

type Connection struct {
	dbox.Connection
	// Sql                  *sql.DB
	Sess                             *odbc.Connection
	Drivername, database, dateFormat string
}

func init() {
	dbox.RegisterConnector("odbc", NewConnection)
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
	ci := c.Info()
	host := ci.Host
	c.database = ci.Database
	username := ci.UserName
	pass := ci.Password
	if ci.Settings != nil {
		c.Drivername = ci.Settings.Get("driver", "").(string)
	}
	c.dateFormat = ci.Settings.Get("dateformat", "").(string)
	ConnectionString := "DSN=" + host + ";UID=" + username + ";PWD=" + pass //DSN=mysql-dsn;UID=root;PWD=root
	e := c.OdbcConnect(ci.Settings.Get("connector", "").(string), ConnectionString)
	if e != nil {
		return err.Error(packageName, modConnection, "Connect", e.Error())
	}
	return nil
}

func (c *Connection) OdbcConnect(connector string, stringConnection string) error {
	odbcconn, e := odbc.Connect(stringConnection)
	if e != nil {
		return err.Error(packageName, modConnection, "ODBC Connect", e.Error())
	}
	c.Sess = odbcconn

	/*sqlcon, er := sql.Open(connector, stringConnection)
	if er != nil {
		return err.Error(packageName, modConnection, "SQL Open", e.Error())
	}
	c.Sql = sqlcon

	er = sqlcon.Ping()
	if er != nil {
		return err.Error(packageName, modConnection, "Ping", e.Error())
	}*/
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
	c.Sess.Close()
	// c.Sql.Close()
}
