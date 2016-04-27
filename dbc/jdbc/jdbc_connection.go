package jdbc

import (
	"database/sql"
	"github.com/eaciit/dbox"
	"github.com/eaciit/dbox/dbc/rdbms"
	err "github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"strings"
)

const (
	packageName   = "eaciit.dbox.dbc.jdbc"
	modConnection = "Connection"
)

type Connection struct {
	dbox.Connection
	Sql sql.DB
	// Sess       *jdbc.Connection
	Drivername string
}

func init() {
	dbox.RegisterConnector("jdbc", NewConnection)
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
	database := ci.Database
	username := ci.UserName
	pass := ci.Password

	var ConnectionString string
	var hostdb string
	if ci.Settings != nil {
		driver := ci.Settings.Get("driver", "").(string)
		jarfile := ci.Settings.Get("jar", "").(string)

		splitdriver := strings.Split(ci.Settings.Get("connector", "").(string), ":")

		if splitdriver[1] == "mysql" {
			ConnectionString = "jdbc:mysql:" //"jdbc:mysql://localhost/test?user=minty&password=greatsqldb"
			hostdb = ConnectionString + "//" + host + "/" + database
		} else if splitdriver[1] == "postgres" {
			ConnectionString = "jdbc:postgresql:"
			hostdb = ConnectionString + "//" + host + "/" + database
		} else if splitdriver[1] == "mssql" {
			ConnectionString = "jdbc:sqlserver:" //"jdbc:sqlserver://ServerName\\sqlexpress;database=DBName;user=UserName;password=Password"
			hostdb = ConnectionString + "//" + host + ";dbname=" + database
		} else if splitdriver[1] == "oracle" {
			ConnectionString = "jdbc:oracle:thin:@" //"jdbc:oracle:thin:@//localhost:1521/orcl", "scott", "tiger"
			hostdb = ConnectionString + "//" + host + "/" + database
		}
		connStr := hostdb

		ConnectionString = hostdb + "?user=" + username + "&pass=" + pass + "&driver=" + driver + "&jar=" + jarfile + "&str=" + connStr
		e := c.JdbcConnect("jdbc", ConnectionString)
		if e != nil {
			return err.Error(packageName, modConnection, "Connect", e.Error())
		}

	} else {
		return err.Error(packageName, modConnection, "Connect", "Settings Connection Empty")
	}

	return nil
}

func (c *Connection) JdbcConnect(drivername string, stringConnection string) error {
	sqlcon, e := sql.Open(drivername, stringConnection)
	if e != nil {
		return err.Error(packageName, modConnection, "Connect", e.Error())
	}
	c.Sql = *sqlcon
	c.Drivername = strings.Split(c.Info().Settings.Get("connector", "").(string), ":")[1]

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

// func (c *Connection) Close() {
// 	c.Sess.Close()
// 	// c.Sql.Close()
// }
// func (c *Connection) Close() {
// 	c.Sql.Close()
// }
