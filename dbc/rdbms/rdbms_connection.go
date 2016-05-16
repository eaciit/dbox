package rdbms

import (
	"database/sql"
	"github.com/eaciit/dbox"
	err "github.com/eaciit/errorlib"
	"github.com/eaciit/hdc/hive"
	"github.com/eaciit/toolkit"
	"strings"
)

const (
	packageName   = "eaciit.dbox.dbc.rdbms"
	modConnection = "Connection"
)

type Connection struct {
	dbox.Connection
	Hive       *hive.Hive
	Sql        sql.DB
	Drivername string
	DateFormat string
}

func (c *Connection) RdbmsConnect(drivername string, stringConnection string) error {
	if drivername == "hive" {
		connInfo := strings.Split(stringConnection, ",")
		c.Hive = hive.HiveConfig(connInfo[0], connInfo[1], connInfo[2], connInfo[3], connInfo[4], connInfo[5])
		c.Drivername = drivername
		c.Hive.Conn.Open()
		e := c.Hive.Conn.TestConnection()
		if e != nil {
			return err.Error(packageName, modConnection, "Connect", e.Error())
		}
	} else {
		sqlcon, e := sql.Open(drivername, stringConnection)
		if e != nil {
			return err.Error(packageName, modConnection, "Connect", e.Error())
		}
		c.Sql = *sqlcon
		c.Drivername = drivername
		e = sqlcon.Ping()
		if e != nil {
			return err.Error(packageName, modConnection, "Connect", e.Error())
		}
	}
	if c.Info().Settings.Has("dateformat") {
		c.DateFormat = toolkit.ToString(c.Info().Settings.Get("dateformat", ""))
	}

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
	if c.GetDriver() == "hive" {
		if c.Hive.Conn.Open() != nil {
			c.Hive.Conn.Close()
		}
	} else {
		c.Sql.Close()
	}
}

func (c *Connection) OnQuery(query string, name string) []string {
	var astr = []string{}

	rows, e := c.Sql.Query(query)
	if e != nil {
		toolkit.Println(e.Error())
		return nil
	}

	defer rows.Close()
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
		astr = append(astr, val[name].(string))
	}

	return astr

}

func (c *Connection) ObjectNames(obj dbox.ObjTypeEnum) []string {
	var astr = []string{}

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

	return astr
}
