package dbox

import (
	"fmt"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
)

type FilterOp string

const (
	modFilter = "FilterBuilder"

	FilterOpAnd = "$and"
	FilterOpOr  = "$or"

	FilterOpEqual       = "$eq"
	FilterOpNoEqual     = "$ne"
	FilterOpContains    = "$contains"
	FilterOpNotContains = "$notcontains"
)

type Filter struct {
	Field string
	Op    string
	Value interface{}
}

type IFilterBuilder interface {
	Build() (interface{}, error)
	BuildFilter(*Filter) (interface{}, error)
	CombineFilter([]toolkit.M) (interface{}, error)

	//-- comparison
	Eq(string, interface{}) *Filter

	//-- conjunction
	And(...*Filter) *Filter
	Or(...*Filter) *Filter
}

type FilterBuilder struct {
	Filters []*Filter
}

func (fb *FilterBuilder) Build() (interface{}, error) {
	if fb.Filters == nil {
		fb.Filters = []*Filter{}
	}
	mfilters := []toolkit.M{}
	for _, f := range fb.Filters {
		fbout, e := fb.BuildFilter(f)
		if e != nil {
			return nil, errorlib.Error(packageName, modFilter, "Build",
				fmt.Sprintf("%v - %s", f, e.Error()))
		}
		mfilters = append(mfilters, fbout.(toolkit.M))
	}
	return fb.CombineFilter(mfilters)
}

func (fb *FilterBuilder) BuildFilter(f *Filter) (interface{}, error) {
	return nil, errorlib.Error(packageName, modFilter, "BuildFilter", errorlib.NotYetImplemented)
}

func (fb *FilterBuilder) CombineFilter(mfs []toolkit.M) (interface{}, error) {
	return nil, errorlib.Error(packageName, modFilter, "BuildFilter", errorlib.NotYetImplemented)
}

func (fb *FilterBuilder) Eq(field string, value interface{}) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = string(FilterOpEqual)
	f.Value = value
	return f
}

func (fb *FilterBuilder) And(fs ...*Filter) *Filter {
	f := new(Filter)
	f.Op = string(FilterOpAnd)
	f.Value = fs
	return f
}

func (fb *FilterBuilder) Or(fs ...*Filter) *Filter {
	f := new(Filter)
	f.Op = string(FilterOpOr)
	f.Value = fs
	return f
}
