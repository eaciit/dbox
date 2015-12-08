package json

import (
	// "fmt"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"os"
	"runtime"
	"strings"
)

const (
	packageName   = "eaciit.dbox.dbc.json"
	modConnection = "Connection"
)

type Connection struct {
	dbox.Connection
	// session *os.File
	filePath, basePath, baseFileName, separator string
	openFile                                    *os.File
}

func init() {
	dbox.RegisterConnector("json", NewConnection)
}

func NewConnection(ci *dbox.ConnectionInfo) (dbox.IConnection, error) {
	if ci.Settings == nil {
		ci.Settings = toolkit.M{}
	}

	c := new(Connection)
	c.SetInfo(ci)
	c.SetFb(dbox.NewFilterBuilder(new(FilterBuilder)))

	return c, nil
}

func (c *Connection) Connect() error {
	ci := c.Info()

	if ci == nil {
		return errorlib.Error(packageName, modConnection, "Connect", "No connection info")
	} else if ci.Host == "" {
		return errorlib.Error(packageName, modConnection, "Connect", "Read file is not initialized")
	}

	c.filePath = ci.Host

	if c.openFile == nil {
		t, e := os.OpenFile(ci.Host, os.O_RDWR, 0)
		if e != nil {
			return errorlib.Error(packageName, modConnection, "Read File", "Cannot open file")
		}
		c.openFile = t
	}

	defaultPath, fileName, sep := c.GetBaseFilepath()
	c.basePath = defaultPath
	c.baseFileName = fileName
	c.separator = sep

	return nil
}

func (c *Connection) NewQuery() dbox.IQuery {
	q := new(Query)
	q.SetConnection(c)
	q.SetThis(q)
	return q
}

func (c *Connection) Close() {
	if c.openFile != nil {
		c.openFile.Close()
	}
}

func (c *Connection) GetBaseFilepath() (string, string, string) {
	getOS := runtime.GOOS
	var separator string

	if getOS == "windows" {
		separator = "\\"
	} else if getOS == "linux" || getOS == "darwin" {
		separator = "/"
	}

	splitString := strings.Split(c.filePath, separator)
	removeLastSlice := splitString[:len(splitString)-1]

	return strings.Join(removeLastSlice, separator), splitString[len(splitString)-1], separator
}
