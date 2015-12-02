package json

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"os"
	"strings"
)

const (
	modCursor = "Cursor"

	QueryResultCursor = "JsonCursor"
	QueryResultPipe   = "JsonPipe"
)

type Cursor struct {
	dbox.Cursor

	ResultType string

	count      int
	jsonCursor interface{}
	tempFile   []byte
	session    *os.File
}

func (c *Cursor) Close() {
	c.session.Close()
}

func (c *Cursor) validate() error {
	if c.ResultType == QueryResultCursor {
		if c.jsonCursor == nil {
			return errors.New("Query cursor is nil")
		}
	}

	return nil
}

func (c *Cursor) prepIter() error {
	e := c.validate()
	fmt.Printf("c.jsonCursor:%v\n", c.jsonCursor)
	if e != nil {
		return e
	}
	if c.jsonCursor == nil {
		if c.ResultType == QueryResultCursor {
			c.jsonCursor = 0
		}
	}
	return nil
}

func (c *Cursor) Count() int {
	return c.count
}

func (c *Cursor) ResetFetch() error {
	c.jsonCursor = c.validate()
	e := c.prepIter()
	if e != nil {
		return errorlib.Error(packageName, modCursor, "ResetFetch", e.Error())
	}
	return nil
}

func (c *Cursor) Fetch(m interface{}, n int, closeWhenDone bool) (
	*dbox.DataSet, error) {
	if closeWhenDone {
		defer c.Close()
	}

	e := c.prepIter()
	if e != nil {
		return nil, errorlib.Error(packageName, modCursor, "Fetch", e.Error())
	}

	if c.jsonCursor == nil {
		return nil, errorlib.Error(packageName, modCursor, "Fetch", "Iter object is not yet initialized")
	}

	datas := []interface{}{}
	dec := json.NewDecoder(strings.NewReader(string(c.tempFile)))
	dec.Decode(&datas)
	ds := dbox.NewDataSet(m)
	if n == 0 {

		ds.Data = datas
	} else if n > 0 {
		fetched := 0
		fetching := true
		for fetching {
			var dataM = toolkit.M{}
			for i := 0; i < len(c.jsonCursor.([]string)); i++ {
				dataM[c.jsonCursor.([]string)[i]] = datas[fetched].(map[string]interface{})[c.jsonCursor.([]string)[i]]

				if len(dataM) == len(c.jsonCursor.([]string)) {
					ds.Data = append(ds.Data, dataM)
				}
			}

			fetched++
			if fetched == n {
				fetching = false
			}
		}
	}

	return ds, nil
}
