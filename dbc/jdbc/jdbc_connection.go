package jdbc

import (
	"database/sql"
	"github.com/eaciit/dbox"
	_ "github.com/eaciit/dbox/dbc/jdbc/driver"
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
			ConnectionString = "jdbc:oracle:thin:@" //"jdbc:oracle:thin:@//localhost:1521:orcl", "scott", "tiger"
			hostdb = ConnectionString + host + ":" + database
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

func (c *Connection) OnQuery(query string, name string) []string {
	var astr = []string{}
	rows, e := c.Sql.Query(query)
	if e != nil {
		toolkit.Println(e.Error())
		return nil
	}
	// defer rows.Close()

	columns, e := rows.Columns()
	if e != nil {
		toolkit.Println(e.Error())
		return nil
	}

	count := len(columns)

	tableData := []toolkit.M{}
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for rows.Next() {
		for i := 0; i < count; i++ {
			valuePtrs[i] = &values[i]
		}

		rows.Scan(valuePtrs...)
		entry := toolkit.M{}

		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry.Set(strings.ToLower(col), v)
		}
		tableData = append(tableData, entry)
	}
	for _, val := range tableData {
		astr = append(astr, val.GetString("table_name"))
	}

	return astr

}

func (c *Connection) ObjectNames(obj dbox.ObjTypeEnum) []string {
	var astr = []string{}
	// toolkit.Println("driverNAmesas :: ", c.Drivername)

	if c.Drivername == "mysql" {
		viewmy := c.OnQuery("SHOW FULL TABLES WHERE TABLE_TYPE LIKE 'VIEW'", "tables_in_"+c.Info().Database)
		tablemy := c.OnQuery("SHOW FULL TABLES IN "+c.Info().Database+" WHERE TABLE_TYPE LIKE '%BASE TABLE%' ", "tables_in_"+c.Info().Database)
		procmy := c.OnQuery("SHOW PROCEDURE STATUS WHERE Db = '"+c.Info().Database+"';", "name")
		if obj == "table" {
			astr = tablemy
		} else if obj == "procedure" {
			astr = procmy
		} else if obj == "view" {
			astr = viewmy
		} else if obj == "allobject" {
			for _, tab := range tablemy {
				astr = append(astr, tab)
			}
			for _, v := range viewmy {
				astr = append(astr, v)
			}
			for _, p := range procmy {
				astr = append(astr, p)
			}
		}
	}
	if c.Drivername == "mssql" {
		tablems := c.OnQuery("SELECT name FROM sys.tables", "name")
		viewms := c.OnQuery("SELECT * FROM sys.views", "name")
		procms := c.OnQuery("SELECT name,type FROM dbo.sysobjects WHERE (type = 'P')", "name")
		if obj == "table" {
			astr = tablems
		} else if obj == "procedure" {
			astr = procms
		} else if obj == "view" {
			astr = viewms
		} else if obj == "allobject" {
			for _, tab := range tablems {
				astr = append(astr, tab)
			}
			for _, v := range viewms {
				astr = append(astr, v)
			}
			for _, p := range procms {
				astr = append(astr, p)
			}
		}
	}
	if c.Drivername == "postgres" {
		tablepg := c.OnQuery("select table_name from information_schema.tables where table_schema = 'public'", "table_name")
		viewpg := c.OnQuery("select table_name from INFORMATION_SCHEMA.views WHERE table_schema = ANY (current_schemas(false))", "table_name")
		procpg := c.OnQuery("SELECT  p.proname FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_proc p ON p.pronamespace = n.oid WHERE n.nspname = 'public'", "proname")
		if obj == "table" {
			astr = tablepg
		} else if obj == "procedure" {
			astr = procpg
		} else if obj == "view" {
			astr = viewpg
		} else if obj == "allobject" {
			for _, tab := range tablepg {
				astr = append(astr, tab)
			}
			for _, v := range viewpg {
				astr = append(astr, v)
			}
			for _, p := range procpg {
				astr = append(astr, p)
			}
		}
	}

	if c.Drivername == "oracle" {
		//astr = c.OnQuery("select table_name from information_schema.tables where table_schema = 'public'", "table_name")
	}
	c.Sql.Close()

	return astr
}
