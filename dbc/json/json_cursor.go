package json

import (
	"encoding/json"
	// "errors"
	"bufio"
	// "fmt"
	"github.com/eaciit/dbox"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
)

const (
	modCursor = "Cursor"

	QueryResultCursor = "JsonCursor"
	QueryResultPipe   = "JsonPipe"
)

type Cursor struct {
	dbox.Cursor
	count, lines            int
	whereFields, jsonSelect interface{}
	readFile                []byte
	session, fetchSession   *os.File
	isWhere                 bool
	tempPathFile            string
}

func (c *Cursor) Close() {
	if c.session != nil || c.fetchSession != nil {
		c.Connection().(*Connection).Close()
		os.Remove(c.tempPathFile)
	}
}

func (c *Cursor) validate() error {
	// c.Close()
	if c.session == nil {
		c.Connection().(*Connection).OpenSession()
		c.session = c.Connection().(*Connection).openFile
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
	// c.Close()

	if c.fetchSession != nil {
		ioutil.WriteFile(c.tempPathFile, []byte(string("")), 0666)
	}

	return nil
}

func (c *Cursor) Fetch(m interface{}, n int, closeWhenDone bool) error {
	if closeWhenDone {
		c.Close()
	}

	e := c.prepIter()
	if e != nil {
		return errorlib.Error(packageName, modCursor, "Fetch", e.Error())
	}

	if c.jsonSelect == nil {
		return errorlib.Error(packageName, modCursor, "Fetch", "Iter object is not yet initialized")
	}

	// var mData []interface{}
	datas := []toolkit.M{}
	dec := json.NewDecoder(strings.NewReader(string(c.readFile)))
	dec.Decode(&datas)
	// ds := dbox.NewDataSet(m)
	if n == 0 {
		whereFieldsToMap, e := toolkit.ToM(c.whereFields)
		if e != nil {
			return errorlib.Error(packageName, modCursor, "Fetch", e.Error())
		}

		b := c.getCondition(whereFieldsToMap)
		var foundSelected = toolkit.M{}
		var foundData = []toolkit.M{}
		var getRemField = toolkit.M{}
		if c.isWhere {
			if b {
				for _, v := range datas {
					for i, subData := range v {
						getRemField[i] = i //append(getRemField, i)
						for _, vWhere := range whereFieldsToMap {
							for _, subWhere := range vWhere.([]interface{}) {
								for _, subsubWhere := range subWhere.(map[string]interface{}) {
									if len(c.jsonSelect.([]string)) == 0 {
										if strings.ToLower(subData.(string)) == strings.ToLower(subsubWhere.(string)) {
											// ds.Data = append(ds.Data, v)
											*(m.(*[]toolkit.M)) = append(*(m.(*[]toolkit.M)), v)
										}
									} else {
										if strings.ToLower(subData.(string)) == strings.ToLower(subsubWhere.(string)) {
											foundData = append(foundData, v)
										}
									}
								}
							}
						}
					}
				}

				itemToRemove := removeDuplicatesUnordered(getRemField, c.jsonSelect.([]string))

				if len(foundData) > 0 {
					var found toolkit.M
					for _, found = range foundData {
						for _, remitem := range itemToRemove {
							found.Unset(remitem)
						}

						// ds.Data = append(ds.Data, found)
						*(m.(*[]toolkit.M)) = append(*(m.(*[]toolkit.M)), found)
					}
				}
			} else {
				for _, v := range datas {
					for _, v2 := range v {
						for _, vWhere := range c.whereFields.(toolkit.M) {
							if reflect.ValueOf(v2).Kind() == reflect.String {
								if strings.ToLower(v2.(string)) == strings.ToLower(vWhere.(string)) {
									if len(c.jsonSelect.([]string)) == 0 {
										// ds.Data = append(ds.Data, v)
										*(m.(*[]toolkit.M)) = append(*(m.(*[]toolkit.M)), v)
									} else {
										// fmt.Println(c.jsonSelect.([]string)[0])
										// fmt.Println(v.(map[string]interface{}))
										foundData = append(foundData, v)
									}
								}
							}

						}
					}
				}

				if len(foundData) > 0 {

					for _, found := range foundData {
						for i, subData := range found {
							for _, selected := range c.jsonSelect.([]string) {
								if strings.ToLower(selected) == strings.ToLower(i) {
									foundSelected[i] = subData
								} else if selected == "*" {
									foundSelected[i] = subData
								}
							}
						}
					}
					// ds.Data = append(ds.Data, foundSelected)
					*(m.(*[]toolkit.M)) = append(*(m.(*[]toolkit.M)), foundSelected)
				}
			}
		} else {
			if c.jsonSelect.([]string)[0] != "*" {
				for _, v := range datas {
					for i, _ := range v {
						getRemField[i] = i
					}
				}

				itemToRemove := removeDuplicatesUnordered(getRemField, c.jsonSelect.([]string))
				for _, found := range datas {
					toMap := toolkit.M(found)
					for _, remitem := range itemToRemove {
						toMap.Unset(remitem)
					}

					*(m.(*[]toolkit.M)) = append(*(m.(*[]toolkit.M)), toMap)
					// *(m.(*[]toolkit.M)) = ds.Data
				}
			} else {
				// ds.Data = datas
				*(m.(*[]toolkit.M)) = datas
			}
		}
	} else if n > 0 {
		fetched := 0
		fetching := true

		///read line
		fetchFile, e := os.OpenFile(c.tempPathFile, os.O_RDWR, 0)
		defer fetchFile.Close()
		if e != nil {
			return errorlib.Error(packageName, modQuery+".Exec", "Fetch file", e.Error())
		}
		c.fetchSession = fetchFile

		scanner := bufio.NewScanner(fetchFile)
		lines := 0
		for scanner.Scan() {
			lines++
		}
		if lines > 0 {
			fetched = lines
			n = n + lines
		}
		for fetching {
			var dataM = toolkit.M{}

			if c.jsonSelect.([]string)[0] != "*" {
				for i := 0; i < len(c.jsonSelect.([]string)); i++ {

					dataM[c.jsonSelect.([]string)[i]] = datas[fetched][c.jsonSelect.([]string)[i]]

					if len(dataM) == len(c.jsonSelect.([]string)) {
						// ds.Data = append(ds.Data, dataM)
						*(m.(*[]toolkit.M)) = append(*(m.(*[]toolkit.M)), dataM)
					}
				}
			} else {
				*(m.(*[]toolkit.M)) = append(*(m.(*[]toolkit.M)), datas[fetched])
			}
			io.WriteString(fetchFile, toolkit.JsonString(dataM)+"\n")

			fetched++
			if fetched == n {

				fetching = false
			}
		}
	}
	// c.Close()
	return nil
}

func (c *Cursor) getCondition(condition toolkit.M) bool {
	var flag bool
	var dataCheck toolkit.M

	for i, v := range condition {
		if i == "$and" || i == "$or" {
			flag = true
		} else if v != dataCheck.Get(i, "").(string) {
			flag = false
		}

	}
	return flag
}

func RemoveDuplicates(xs *[]string) {
	found := make(map[string]bool)
	j := 0
	for i, x := range *xs {
		if !found[x] {
			found[x] = true
			(*xs)[j] = (*xs)[i]
			j++
		}
	}
	*xs = (*xs)[:j]
}

func removeDuplicatesUnordered(elements toolkit.M, key []string) []string {
	for _, k := range key {
		elements.Unset(k)
	}

	result := []string{}
	for key, _ := range elements {
		result = append(result, key)
	}
	return result
}
