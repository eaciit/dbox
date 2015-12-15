package csv

import (
	"encoding/csv"
	// "fmt"
	"github.com/eaciit/cast"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"io"
	"os"
	"regexp"
	"strings"
	// "time"
	// "reflect"
)

type TypeOpenFile_Enum int

type headerstruct struct {
	name     string
	dataType string
}

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

	headerColumn []headerstruct
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

	useHeader := ci.Settings.Get("useheader", false).(bool)

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
	c.SetHeaderData(useHeader)

	// fmt.Println(c.headerColumn)
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

}

func (c *Connection) SetHeaderData(useHeader bool) {
	ci := c.Info()
	dateformat := ci.Settings.Get("dateformat", "").(string)

	var tempstruct []headerstruct

	tempData, e := c.reader.Read()
	for i, v := range tempData {
		ts := headerstruct{}
		ts.name = string(i)
		ts.dataType = "string"
		if useHeader {
			ts.name = v
		}
		tempstruct = append(tempstruct, ts)
	}
	if useHeader {
		tempData, e = c.reader.Read()
	}

	isCheckType := true
	ix := 0
	for isCheckType && e != io.EOF {
		ix += 1
		isCheckType = false

		for i, v := range tempData {
			if v != "" {
				matchNumber := false
				matchFloat := false
				matchDate := false

				//dd.mm.yyyy dd/mm/yyyy dd-mm-yyyy
				//yyyy.mm.dd yyyy/mm/dd yyyy-mm-dd
				formatDate := "((^(0[0-9]|[0-9]|(1|2)[0-9]|3[0-1])(\\.|\\/|-)(0[0-9]|[0-9]|1[0-2])(\\.|\\/|-)[\\d]{4}$)|(^[\\d]{4}(\\.|\\/|-)(0[0-9]|[0-9]|1[0-2])(\\.|\\/|-)(0[0-9]|[0-9]|(1|2)[0-9]|3[0-1])$))"
				matchDate, _ = regexp.MatchString(formatDate, v)
				if !matchDate && dateformat != "" {
					d := cast.String2Date(v, dateformat)
					if d.Year() > 1 {
						matchDate = true
					}
				}

				x := strings.Index(v, ".")

				if x > 0 {
					matchFloat = true
					v = strings.Replace(v, ".", "", 1)
				}

				matchNumber, _ = regexp.MatchString("^\\d+$", v)

				tempstruct[i].dataType = "string"
				if matchNumber {
					tempstruct[i].dataType = "int"
					if matchFloat {
						tempstruct[i].dataType = "float"
					}
				}

				if matchDate {
					tempstruct[i].dataType = "date"
				}
			}
		}
		for _, v := range tempstruct {
			if v.dataType == "" {
				isCheckType = true
			}
		}

		if isCheckType {
			tempData, _ = c.reader.Read()
		}

		// fmt.Println(ix, "-", isCheckType)
		// fmt.Println(tempstruct)
		if ix > 5 {
			break
		}
	}

	c.headerColumn = tempstruct

	c.file.Close()
	c.file, _ = os.Open(ci.Host)
	c.reader = csv.NewReader(c.file)
	c.SetReaderParam()

	if useHeader {
		tempData, _ = c.reader.Read()
	}
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
	c.writer = nil
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
