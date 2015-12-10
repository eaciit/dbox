package csv

import (
	"encoding/csv"
	"github.com/eaciit/dbox"
	//	"io"
	"os"

	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	//	"time"
)

type TypeOpenFile_Enum int

const (
	TypeOpenFile_Append TypeOpenFile_Enum = iota
	TypeOpenFile_Create
)

const (
	packageName   = "eaciit.dbox.dbc.csv"
	modConnection = "Connection"
)

type Connection struct {
	dbox.Connection

	TypeOpenFile TypeOpenFile_Enum
	ExecOpr      bool

	file     *os.File
	tempfile *os.File
	reader   *csv.Reader
	writer   *csv.Writer

	headerColumn []string
}

func init() {
	dbox.RegisterConnector("csv", NewConnection)
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

/*
	file				string	// File Path => host

	useheader			bool	// field column, harus ada
	comma           	rune 	// field delimiter (set to ',' by NewReader)
	comment          	rune 	// comment character for start of line
	fieldsperrecord  	int  	// number of expected fields per record
	lazyquotes       	bool 	// allow lazy quotes
	trailingcomma    	bool 	// ignored; here for backwards compatibility
	trimleadingspace 	bool
*/
func (c *Connection) Connect() error {
	ci := c.Info()
	if ci == nil {
		return errorlib.Error(packageName, modConnection, "Connect", "ConnectionInfo is not initialized")
	}

	if useHeader := ci.Settings.Get("useheader", false).(bool); !useHeader {
		return errorlib.Error(packageName, modConnection, "Connect", "Header is not set")
	}

	if filePath := ci.Host; filePath != "" {
		var err error
		c.file, err = os.Open(filePath)
		if err != nil {
			return errorlib.Error(packageName, modConnection, "Connect", "Cannot Open File")
		}
		c.reader = csv.NewReader(c.file)
	} else {
		return errorlib.Error(packageName, modConnection, "Connect", "File is not initialized")
	}

	c.SetReaderParam()

	return nil
}

func (c *Connection) SetReaderParam() {
	ci := c.Info()

	if delimiter := ci.Settings.Get("delimiter", "").(string); delimiter != "" {
		c.reader.Comma = rune(delimiter[0])
	}

	if comment := ci.Settings.Get("comment", "").(string); comment != "" {
		c.reader.Comment = rune(comment[0])
	}

	if fieldsPerRecord := ci.Settings.GetInt("fieldsperrecord"); fieldsPerRecord > 0 {
		c.reader.FieldsPerRecord = fieldsPerRecord
	}

	if lazyQuotes := ci.Settings.Get("lazyquotes", false).(bool); lazyQuotes {
		c.reader.LazyQuotes = lazyQuotes
	}

	if trailingComma := ci.Settings.Get("trailingcomma", false).(bool); trailingComma {
		c.reader.TrailingComma = trailingComma
	}

	if trimLeadingSpace := ci.Settings.Get("trimleadingspace", false).(bool); trimLeadingSpace {
		c.reader.TrailingComma = trimLeadingSpace
	}

	c.headerColumn, _ = c.reader.Read()
}

func (c *Connection) NewQuery() dbox.IQuery {
	q := new(Query)
	q.SetConnection(c)
	q.SetThis(q)
	return q
}

func (c *Connection) Close() {
	if c.file != nil {
		c.file.Close()
	}
}

func (c *Connection) StartSessionWrite() error {
	c.Close()

	ci := c.Info()
	if ci == nil {
		return errorlib.Error(packageName, modConnection, "Connect", "ConnectionInfo is not initialized")
	}

	if filePath := ci.Host; filePath != "" {
		var err error

		c.file, err = os.OpenFile(filePath, os.O_RDWR|os.O_APPEND, 0)
		if err != nil {
			return errorlib.Error(packageName, modConnection, "SessionWrite", "Cannot Open File")
		}

		if c.TypeOpenFile == TypeOpenFile_Create {
			c.reader = csv.NewReader(c.file)
			c.SetReaderParam()

			c.tempfile, err = os.OpenFile(filePath+".temp", os.O_CREATE, 0)
			c.writer = csv.NewWriter(c.tempfile)
		} else {
			c.writer = csv.NewWriter(c.file)
		}
	}

	if delimiter := ci.Settings.Get("delimiter", "").(string); delimiter != "" {
		c.writer.Comma = rune(delimiter[0])
	}

	return nil
}

func (c *Connection) EndSessionWrite() error {
	c.Close()
	if c.TypeOpenFile == TypeOpenFile_Create {
		c.tempfile.Close()
		if c.ExecOpr {
			os.Remove(c.Info().Host)
			os.Rename(c.Info().Host+".temp", c.Info().Host)
		}
	}

	e := c.Connect()
	if e != nil {
		return errorlib.Error(packageName, modConnection, "SessionWrite", "Reopen Read File")
	}

	return nil

}
