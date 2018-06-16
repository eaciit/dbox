package hivetr

import (
	"errors"

	"github.com/eaciit/errorlib"
	"github.com/kharism/dbox"
)

// dummy filter builder, does not do anything, just so that this dbc if
type FilterBuilder struct {
}

func (f *FilterBuilder) SetThis(fb dbox.IFilterBuilder) dbox.IFilterBuilder {
	return f
}
func (f *FilterBuilder) Build() (interface{}, error) {
	return nil, errors.New(errorlib.NotYetImplemented)
}
func (f *FilterBuilder) BuildFilter(a *dbox.Filter) (interface{}, error) {
	return nil, errors.New(errorlib.NotYetImplemented)
}
func (f *FilterBuilder) CombineFilters(a []interface{}) (interface{}, error) {
	return nil, errors.New(errorlib.NotYetImplemented)
}

func (f *FilterBuilder) AddFilter(a ...*dbox.Filter) {}
