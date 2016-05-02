package jsons

import (
	"github.com/eaciit/dbox"
	err "github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"os"
	"regexp"
	"strings"
)

func init() {
	dbox.RegisterConnector("jsons", NewConnection)
}

func NewConnection(ci *dbox.ConnectionInfo) (dbox.IConnection, error) {
	if ci.Settings == nil {
		ci.Settings = toolkit.M{}
	}
	c := new(Connection)
	c.folder = ci.Host
	c.SetInfo(ci)
	c.SetFb(dbox.NewFilterBuilder(new(FilterBuilder)))
	return c, nil
}

type Connection struct {
	dbox.Connection

	folder      string
	defautQuery *Query
}

func (c *Connection) Connect() error {
	c.folder = c.Info().Host
	if c.folder == "" {
		return err.Error(packageName, modConnection, "Connect", "folder path is empty")
	}

	_, e := os.Stat(c.folder)
	if e != nil {
		return err.Error(packageName, modConnection, "Connect",
			e.Error())
	}

	return nil
}

func (c *Connection) NewQuery() dbox.IQuery {
	pooling := c.Info().Settings.Get("pooling", false).(bool)

	if pooling && c.defautQuery != nil {
		return c.defautQuery
	} else {
		q := new(Query)
		q.SetConnection(c)
		q.SetThis(q)
		/*
			if pooling {
				c.defautQuery = q
			}
		*/
		return q
	}
	return nil
}

func (c *Connection) ObjectNames(obj dbox.ObjTypeEnum) []string {
	if obj == "" {
		obj = dbox.ObjTypeAll
	}

	astr := []string{}

	if obj == dbox.ObjTypeAll || obj == dbox.ObjTypeTable {
		d, err := os.Open(c.Info().Host)
		if err != nil {
			return []string{}
		}
		defer d.Close()

		files, err := d.Readdir(-1)
		if err != nil {
			return []string{}
		}

		for _, file := range files {
			if cond, _ := regexp.MatchString("^(.*)(\\.[Jj][Ss][Oo][Nn])$", file.Name()); cond {
				astr = append(astr, file.Name()[:strings.IndexAny(file.Name(), ".")])
			}
		}

	}
	return astr
}

func (c *Connection) Close() {
}
