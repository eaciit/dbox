package dbox

import (
	"github.com/eaciit/toolkit"
)

type Filter struct {
	Field string
	Op    string
	Value string
}

type IFilterBuilder interface {
	Build() interface{}
	BuildFilter(*Filter) interface{}
	CombineFilter([]toolkit.M) interface{}
}

type FilterBuilder struct {
	Filters []*Filter
}

func (fb *FilterBuilder) Build() interface{} {
	if fb.Filters == nil {
		fb.Filters = []*Filter{}
	}
	mfilters := []toolkit.M{}
	for _, f := range fb.Filters {
		mfilters = append(mfilters, fb.BuildFilter(f))
	}
	return fb.CombineFilter(mfilters)
}

func (fb *FilterBuilder) BuildFilter(f *filter) interface{} {
	return ""
}

func (fb *FilterBuilder) CombineFilter(mfs []toolkit.M) interface{} {
	return ""
}
