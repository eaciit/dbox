package json

import (
	"encoding/json"
	"fmt"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	// "io"
	"io/ioutil"
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
	filePath, basePath, baseFileName,
	separator, tempPathFile, dataType string
	openFile, fetchSession *os.File
	writer                 *json.Encoder
	isNewSave              bool
	lines                  int
	getJsonToMap           toolkit.Ms
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
	c.Close()

	ci := c.Info()

	if ci == nil || ci.Host == "" {
		return errorlib.Error(packageName, modConnection, "Connect", "No connection info")
	}

	_, e := os.Stat(ci.Host)
	if ci.Settings != nil {
		if ci.Settings["newfile"] == true {
			if os.IsNotExist(e) {
				create, _ := os.Create(ci.Host)
				create.Close()
			}
		} else {
			if os.IsNotExist(e) {
				return errorlib.Error(packageName, modConnection, "Connect", "Create new file is false")
			}
		}
	} else if os.IsNotExist(e) {
		return errorlib.Error(packageName, modConnection, "Connect", "No json file found")
	}

	c.filePath = ci.Host

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

func (c *Connection) OpenSession() error {
	c.Close()

	t, e := os.OpenFile(c.filePath, os.O_RDWR, 0)
	if e != nil {
		return errorlib.Error(packageName, modConnection, "Open File", e.Error())
	}
	c.openFile = t

	i, e := ioutil.ReadFile(c.filePath)
	if e != nil {
		return errorlib.Error(packageName, modQuery+".Exec", "Read file", e.Error())
	}

	var hoomanJson toolkit.Ms
	e = toolkit.Unjson(i, &hoomanJson)
	if e != nil {
		return errorlib.Error(packageName, modQuery+".Exec", "Cannot Unjson", e.Error())
	}

	if len(hoomanJson) == 0 {
		c.isNewSave = true
	} else {
		c.getJsonToMap = hoomanJson
	}

	return nil
}

func (c *Connection) WriteSession() error {
	c.Close()

	///create temp json file
	tempPathFile := c.basePath + c.separator + "temp_" + c.baseFileName
	create, _ := os.Create(tempPathFile)
	create.Close()

	t, e := os.OpenFile(c.basePath+c.separator+"temp_"+c.baseFileName, os.O_RDWR, 0)
	if e != nil {
		return errorlib.Error(packageName, modConnection, "Read and write File", "Cannot read and write file")
	}
	c.openFile = t
	return nil
}

func (c *Connection) CloseWriteSession() error {
	c.Close()

	eRem := os.Remove(c.filePath)
	if eRem != nil {
		return errorlib.Error(packageName, modQuery+".Exec", "Close Write Session", eRem.Error())
	}

	eRen := os.Rename(c.basePath+c.separator+"temp_"+c.baseFileName, c.filePath)
	if eRen != nil {
		return errorlib.Error(packageName, modQuery+".Exec", "Close Write Session", eRen.Error())
	}
	return nil
}

func (c *Connection) FetchSession() error {
	///create temp text file
	basePath, _, sep := c.GetBaseFilepath()
	tempPathFile := fmt.Sprintf("%s%sfetch.temp", basePath, sep) //basePath + sep + "fetch.temp"
	_, e := os.Stat(tempPathFile)
	if os.IsNotExist(e) {
		create, _ := os.Create(tempPathFile)
		create.Close()
	}
	c.tempPathFile = tempPathFile
	return nil
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

/*func (c *Connection) RemLastLine(filename, methodType string) string {
	var (
		s         string
		delimiter byte
	)
	i, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	s = string(i)
	if methodType == "hasNewSave" {
		delimiter = ','
	} else if methodType == "hasSave" {
		delimiter = ']'
	}
	if last := len(s) - 1; last >= 0 && s[last] == delimiter {
		s = s[:last]
	}
	return s
}
*/
