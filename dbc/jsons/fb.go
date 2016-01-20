package jsons

import (
	"github.com/eaciit/dbox"
)

type FilterBuilder struct {
	dbox.FilterBuilder
}

func (fb *FilterBuilder) BuildFilter(f *dbox.Filter) (interface{}, error) {
	return nil, nil
}

func (fb *FilterBuilder) CombineFilters(fs []interface{}) (interface{}, error) {
	return nil, nil
}
