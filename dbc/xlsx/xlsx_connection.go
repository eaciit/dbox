package xlsx

import (
	// "fmt"
	"github.com/eaciit/cast"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"github.com/tealeg/xlsx"
	// "io"
	// "os"
	// "regexp"
	// "strings"
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
	packageName   = "eaciit.dbox.dbc.xlsx"
	modConnection = "Connection"
)

type Connection struct {
	dbox.Connection

	TypeOpenFile TypeOpenFile_Enum
	ExecOpr      bool
	setNewHeader bool
	isMapHeader  bool

	reader *xlsx.File
	writer *xlsx.File

	headerColumn []headerstruct
	rowstart     int
}

func init() {
	dbox.RegisterConnector("xlsx", NewConnection)
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
		return errorlib.Error(packageName, modConnection, "Connect", "ConnectionInfo is not initialized")
	}

	useHeader := ci.Settings.Get("useheader", false).(bool)
	rowstart := ci.Settings.Get("rowstart", 0).(int)
	isNewFile := ci.Settings.Get("newfile", false).(bool)
	c.setNewHeader = false

	c.rowstart = rowstart

	if filePath := ci.Host; filePath != "" {
		var err error
		c.reader, err = xlsx.OpenFile(filePath)
		if err != nil {
			if isNewFile {
				c.writer = xlsx.NewFile()
				err = c.writer.Save(filePath)
				if err != nil {
					return errorlib.Error(packageName, modConnection, "Connect", "Cannot Create New File")
				}
				c.writer = nil
				c.reader, err = xlsx.OpenFile(filePath)
				if useHeader {
					c.setNewHeader = true
				}
			}

			if !isNewFile || err != nil {
				return errorlib.Error(packageName, modConnection, "Connect", "Cannot Open File")
			}
		}
	} else {
		return errorlib.Error(packageName, modConnection, "Connect", "File is not initialized")
	}

	// c.SetReaderParam()
	// if !c.setNewHeader {
	c.SetHeaderData(useHeader)
	// }

	c.isMapHeader = false
	if ci.Settings.Has("mapheader") {
		c.isMapHeader = true
		var tempstruct []headerstruct
		tMapHeader := ci.Settings["mapheader"].([]toolkit.M)
		for _, val := range tMapHeader {
			ts := headerstruct{}
			for cols, dataType := range val {
				ts.name = cols
				ts.dataType = dataType.(string)
			}
			tempstruct = append(tempstruct, ts)
		}
		c.headerColumn = tempstruct

		if c.setNewHeader {
			//write new header
		}
	}

	return nil
}

// func (c *Connection) SetReaderParam() {
// 	ci := c.Info()

// 	if delimiter := ci.Settings.Get("delimiter", "").(string); delimiter != "" {
// 		c.reader.Comma = rune(delimiter[0])
// 	}

// 	if comment := ci.Settings.Get("comment", "").(string); comment != "" {
// 		c.reader.Comment = rune(comment[0])
// 	}

// 	if fieldsPerRecord := ci.Settings.GetInt("fieldsperrecord"); fieldsPerRecord > 0 {
// 		c.reader.FieldsPerRecord = fieldsPerRecord
// 	}

// 	if lazyQuotes := ci.Settings.Get("lazyquotes", false).(bool); lazyQuotes {
// 		c.reader.LazyQuotes = lazyQuotes
// 	}

// 	if trailingComma := ci.Settings.Get("trailingcomma", false).(bool); trailingComma {
// 		c.reader.TrailingComma = trailingComma
// 	}

// 	if trimLeadingSpace := ci.Settings.Get("trimleadingspace", false).(bool); trimLeadingSpace {
// 		c.reader.TrailingComma = trimLeadingSpace
// 	}

// }

func (c *Connection) SetHeaderData(useHeader bool) {
	ci := c.Info()
	var headerrows int
	headerrows = 5
	// startdatarows := 0
	if ci.Settings.Has("headerrows") {
		// headerrows = cast.ToInt(ci.Settings["headerrows"])
	}
	// Dummy :
	var tempstruct []headerstruct
	n := 1
	for i, _ := range c.reader.Sheet["HIST"].Rows[headerrows].Cells {
		_ = i
		ts := headerstruct{}
		ts.name = cast.ToString(n)
		ts.dataType = "string"

		tempstruct = append(tempstruct, ts)
		n += 1
	}

	c.headerColumn = tempstruct
	// for i, v := range tempData {
	// 	ts := headerstruct{}
	// 	ts.name = string(i)
	// 	ts.dataType = "string"
	// 	if useHeader {
	// 		ts.name = v
	// 	}
	// 	tempstruct = append(tempstruct, ts)
	// }
	// if useHeader && e != io.EOF {
	// 	tempData, e = c.reader.Read()
	// }

	// isCheckType := true
	// ix := 0
	// for isCheckType && e != io.EOF {
	// 	ix += 1
	// 	isCheckType = false

	// 	for i, v := range tempData {
	// 		if v != "" {
	// 			matchNumber := false
	// 			matchFloat := false
	// 			matchDate := false

	// 			formatDate := "((^(0[0-9]|[0-9]|(1|2)[0-9]|3[0-1])(\\.|\\/|-)(0[0-9]|[0-9]|1[0-2])(\\.|\\/|-)[\\d]{4}$)|(^[\\d]{4}(\\.|\\/|-)(0[0-9]|[0-9]|1[0-2])(\\.|\\/|-)(0[0-9]|[0-9]|(1|2)[0-9]|3[0-1])$))"
	// 			matchDate, _ = regexp.MatchString(formatDate, v)
	// 			if !matchDate && dateformat != "" {
	// 				d := cast.String2Date(v, dateformat)
	// 				if d.Year() > 1 {
	// 					matchDate = true
	// 				}
	// 			}

	// 			x := strings.Index(v, ".")

	// 			if x > 0 {
	// 				matchFloat = true
	// 				v = strings.Replace(v, ".", "", 1)
	// 			}

	// 			matchNumber, _ = regexp.MatchString("^\\d+$", v)

	// 			tempstruct[i].dataType = "string"
	// 			if matchNumber {
	// 				tempstruct[i].dataType = "int"
	// 				if matchFloat {
	// 					tempstruct[i].dataType = "float"
	// 				}
	// 			}

	// 			if matchDate {
	// 				tempstruct[i].dataType = "date"
	// 			}
	// 		}
	// 	}
	// 	for _, v := range tempstruct {
	// 		if v.dataType == "" {
	// 			isCheckType = true
	// 		}
	// 	}

	// 	if isCheckType {
	// 		tempData, _ = c.reader.Read()
	// 	}

	// 	// fmt.Println(ix, "-", isCheckType)
	// 	// fmt.Println(tempstruct)
	// 	if ix > 5 {
	// 		break
	// 	}
	// }

	// c.headerColumn = tempstruct

	// c.file.Close()
	// c.file, _ = os.Open(ci.Host)
	// c.reader = csv.NewReader(c.file)
	// c.SetReaderParam()

	// if useHeader {
	// 	tempData, _ = c.reader.Read()
	// }
}

func (c *Connection) NewQuery() dbox.IQuery {
	q := new(Query)
	q.SetConnection(c)
	q.SetThis(q)
	return q
}

func (c *Connection) Close() {
	if c.reader != nil {
		c.reader = nil
	}
}

// func (c *Connection) StartSessionWrite() error {
// 	c.Close()

// 	ci := c.Info()
// 	if ci == nil {
// 		return errorlib.Error(packageName, modConnection, "Connect", "ConnectionInfo is not initialized")
// 	}

// 	if filePath := ci.Host; filePath != "" {
// 		var err error

// 		c.file, err = os.OpenFile(filePath, os.O_RDWR|os.O_APPEND, 0)
// 		if err != nil {
// 			return errorlib.Error(packageName, modConnection, "SessionWrite", "Cannot Open File")
// 		}

// 		if c.TypeOpenFile == TypeOpenFile_Create {
// 			c.reader = csv.NewReader(c.file)
// 			c.SetReaderParam()

// 			c.tempfile, err = os.OpenFile(filePath+".temp", os.O_CREATE, 0)
// 			c.writer = csv.NewWriter(c.tempfile)
// 		} else {
// 			c.writer = csv.NewWriter(c.file)
// 		}
// 	}

// 	if delimiter := ci.Settings.Get("delimiter", "").(string); delimiter != "" {
// 		c.writer.Comma = rune(delimiter[0])
// 	}

// 	return nil
// }

// func (c *Connection) EndSessionWrite() error {
// 	c.Close()
// 	c.writer = nil
// 	if c.TypeOpenFile == TypeOpenFile_Create {
// 		c.tempfile.Close()
// 		if c.ExecOpr {
// 			os.Remove(c.Info().Host)
// 			os.Rename(c.Info().Host+".temp", c.Info().Host)
// 		}
// 	}

// 	e := c.Connect()
// 	if e != nil {
// 		return errorlib.Error(packageName, modConnection, "SessionWrite", "Reopen Read File")
// 	}

// 	return nil

// }
