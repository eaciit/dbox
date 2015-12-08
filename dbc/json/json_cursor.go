package json

import (
	"encoding/json"
	"errors"
	// "fmt"
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
	readFile   []byte
	session    *os.File
	isWhere    bool
}

func (c *Cursor) Close() {
	if c.session != nil {
		c.session.Close()
	}
}

func (c *Cursor) validate() error {
	if c.ResultType == QueryResultCursor {

		if c.readFile == nil {
			return errors.New("Query cursor is nil")
		}
	}

	return nil
}

func (c *Cursor) prepIter() error {
	e := c.validate()

	if e != nil {
		return e
	}
	return nil
}

func (c *Cursor) Count() int {
	return c.count
}

func (c *Cursor) ResetFetch() error {
	c.Close()

	_, e := prepareConnection()
	if e != nil {
		return errorlib.Error(packageName, modCursor, "ResetFetch", e.Error())
	}
	return nil
}

func (c *Cursor) Fetch(m interface{}, n int, closeWhenDone bool) (
	*dbox.DataSet, error) {
	if closeWhenDone {
		c.Close()
	}

	e := c.prepIter()
	if e != nil {
		return nil, errorlib.Error(packageName, modCursor, "Fetch", e.Error())
	}

	if c.jsonCursor == nil {
		return nil, errorlib.Error(packageName, modCursor, "Fetch", "Iter object is not yet initialized")
	}

	datas := []interface{}{}
	dec := json.NewDecoder(strings.NewReader(string(c.readFile)))
	dec.Decode(&datas)
	ds := dbox.NewDataSet(m)
	if n == 0 {
		if c.isWhere {

			for _, v := range datas {
				for _, v2 := range v.(map[string]interface{}) {
					for _, vWhere := range c.jsonCursor.(toolkit.M) {
						if strings.ToLower(v2.(string)) == strings.ToLower(vWhere.(string)) {
							ds.Data = append(ds.Data, v)
						}
					}
				}
			}
		} else {
			ds.Data = datas
		}
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
	c.Close()
	return ds, nil
}
