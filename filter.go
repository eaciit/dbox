package dbox

import (
	"fmt"
	"github.com/eaciit/errorlib"
	//"github.com/eaciit/toolkit"
)

type FilterOp string

const (
	modFilter = "FilterBuilder"

	FilterOpAnd = "$and"
	FilterOpOr  = "$or"

	FilterOpEqual    = "$eq"
	FilterOpNoEqual  = "$ne"
	FilterOpContains = "$contains"

	FilterOpGt  = "$gt"
	FilterOpLt  = "$lt"
	FilterOpGte = "$gte"
	FilterOpLte = "$lte"

	FilterOpIn  = "$in"
	FilterOpNin = "$nin"
)

type Filter struct {
	Field string
	Op    string
	Value interface{}
}

type IFilterBuilder interface {
	SetThis(IFilterBuilder) IFilterBuilder
	Build() (interface{}, error)
	BuildFilter(*Filter) (interface{}, error)
	CombineFilters([]interface{}) (interface{}, error)
	AddFilter(...*Filter)
}

type FilterBuilder struct {
	Filters []*Filter

	thisFb IFilterBuilder
}

func NewFilterBuilder(i IFilterBuilder) IFilterBuilder {
	i.SetThis(i)
	return i
}

func (fb *FilterBuilder) SetThis(i IFilterBuilder) IFilterBuilder {
	fb.thisFb = i
	return i
}

func (fb *FilterBuilder) this() IFilterBuilder {
	if fb.thisFb == nil {
		return fb
	} else {
		return fb.thisFb
	}
}

func (fb *FilterBuilder) AddFilter(fs ...*Filter) {
	if fb.Filters == nil {
		fb.Filters = []*Filter{}
	}

	for _, f := range fs {
		fb.Filters = append(fb.Filters, f)
	}
}

func (fb *FilterBuilder) Build() (interface{}, error) {
	if fb.Filters == nil {
		fb.Filters = []*Filter{}
	}
	mfilters := []interface{}{}
	for _, f := range fb.Filters {
		fbout, e := fb.this().BuildFilter(f)
		if e != nil {
			return nil, errorlib.Error(packageName, modFilter, "Build",
				fmt.Sprintf("%v - %s", f, e.Error()))
		}
		mfilters = append(mfilters, fbout)
	}
	return fb.this().CombineFilters(mfilters)
}

func (fb *FilterBuilder) BuildFilter(f *Filter) (interface{}, error) {
	return nil, errorlib.Error(packageName, modFilter, "BuildFilter", errorlib.NotYetImplemented)
}

func (fb *FilterBuilder) CombineFilters(mfs []interface{}) (interface{}, error) {
	return nil, errorlib.Error(packageName, modFilter, "BuildFilter", errorlib.NotYetImplemented)
}

func Eq(field string, value interface{}) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = string(FilterOpEqual)
	f.Value = value
	return f
}

func Contains(field string, value interface{}) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = string(FilterOpContains)
	f.Value = value
	return f
}

func And(fs ...*Filter) *Filter {
	f := new(Filter)
	f.Op = string(FilterOpAnd)
	f.Value = fs
	return f
}

func Or(fs ...*Filter) *Filter {
	f := new(Filter)
	f.Op = string(FilterOpOr)
	f.Value = fs
	return f
}

func Ne(field string, value interface{}) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = string(FilterOpNoEqual)
	f.Value = value
	return f
}

func In(field string, value []interface{}) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = string(FilterOpIn)
	f.Value = value
	return f
}

func Nin(field string, value []interface{}) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = string(FilterOpNin)
	f.Value = value
	return f
}

func Gt(field string, value interface{}) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = string(FilterOpGt)
	f.Value = value
	return f
}

func Gte(field string, value interface{}) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = string(FilterOpGte)
	f.Value = value
	return f
}

func Lt(field string, value interface{}) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = string(FilterOpLt)
	f.Value = value
	return f
}

func Lte(field string, value interface{}) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = string(FilterOpLte)
	f.Value = value
	return f
}
