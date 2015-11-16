package dbox

import (
	"github.com/eaciit/toolkit"
)

type DataSet struct {
	Model func() interface{}
	Data  []interface{}
}

func NewDataSet(m interface{}) *DataSet {
	if m == nil {
		m = toolkit.M{}
	}

	fn := func() interface{} {
		return m
	}

	ds := new(DataSet)
	ds.Data = []interface{}{}
	ds.Model = fn
	return ds
}
