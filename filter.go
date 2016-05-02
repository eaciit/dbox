package dbox

import (
	"fmt"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"strings"
	//"time"
)

type FilterOp string

const (
	DataString string = "string"
	DataInt    string = "int"
	DataFloat  string = "float"
	DataDate   string = "date"

	modFilter = "FilterBuilder"

	FilterOpAnd = "$and"
	FilterOpOr  = "$or"

	FilterOpEqual    = "$eq"
	FilterOpNoEqual  = "$ne"
	FilterOpContains = "$contains"

	FilterOpStartWith = "$startwith"
	FilterOpEndWith   = "$endwith"

	FilterOpGt  = "$gt"
	FilterOpLt  = "$lt"
	FilterOpGte = "$gte"
	FilterOpLte = "$lte"

	FilterOpIn  = "$in"
	FilterOpNin = "$nin"
)

var DataFloats = []string{"float", "float16", "float32", "float64"}

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
	fb.Filters = []*Filter{}
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

func Ne(field string, value interface{}) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = string(FilterOpNoEqual)
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

func In(field string, invalues ...interface{}) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = string(FilterOpIn)
	f.Value = invalues
	return f
}

func Nin(field string, invalues ...interface{}) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = string(FilterOpNin)
	f.Value = invalues
	return f
}

func Contains(field string, values ...string) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = string(FilterOpContains)
	f.Value = values
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

func Startwith(field string, values string) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = string(FilterOpStartWith)
	f.Value = values
	return f
}

func Endwith(field string, values string) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = string(FilterOpEndWith)
	f.Value = values
	return f
}

/*
ParseFilter, parse filter data and convert it into *Filter
fieldid = name of the field
filter = filter text
dataType = type of data It accepts (string, int, float, date)
*/
func ParseFilter(fieldid string, filterText string, dataType string, dateFormat string) *Filter {
	var fs []*Filter

	fByCommas := strings.Split(filterText, ",")
	for _, fByComma := range fByCommas {
		if strings.HasPrefix(fByComma, "!") {
			fs = append(fs, Ne(fieldid, toInterface(fByComma[1:], dataType, dateFormat)))
		} else if strings.HasPrefix(fByComma, "*") || strings.HasSuffix(fByComma, "*") {
			if !strings.HasPrefix(fByComma, "*") {
				fs = append(fs, Startwith(fieldid, fByComma[:len(fByComma)]))
			} else if !strings.HasSuffix(fByComma, "*") {
				fs = append(fs, Endwith(fieldid, fByComma[1:]))
			} else {
				fs = append(fs, Contains(fieldid, fByComma[1:len(fByComma)-1]))
			}
		} else if strings.Contains(fByComma, "..") {
			bounds := strings.Split(fByComma, "..")
			if len(bounds) == 2 {
				if bounds[0] == "" {
					upperBound := toInterface(bounds[1], dataType, dateFormat)
					fs = append(fs, Lte(fieldid, upperBound))
				} else if bounds[1] == "" {
					lowerBound := toInterface(bounds[0], dataType, dateFormat)
					fs = append(fs, Gte(fieldid, lowerBound))
				} else {
					lowerBound := toInterface(bounds[0], dataType, dateFormat)
					// toolkit.Println(lowerBound.(float64))
					upperBound := toInterface(bounds[1], dataType, dateFormat)
					fs = append(fs, And(Gte(fieldid, lowerBound), Lte(fieldid, upperBound)))
				}
			} else if len(bounds) == 1 {
				lowerBound := toInterface(bounds[0], dataType, dateFormat)
				fs = append(fs, Gte(fieldid, lowerBound))
			}
		} else {
			fs = append(fs, Eq(fieldid, toInterface(fByComma, dataType, dateFormat)))
		}
	}

	if len(fs) == 0 {
		return nil
	} else if len(fs) == 1 {
		return fs[0]
	} else {
		return And(fs...)
	}
}

func toInterface(data string, dataType string, dateFormat string) interface{} {
	if dataType == "" {
		if strings.HasPrefix(data, "#") && strings.HasSuffix(data, "#") {
			dataType = DataDate
		} else {
			vfloat := toolkit.ToFloat64(dataType, 2, toolkit.RoundingAuto)
			vint := toolkit.ToInt(dataType, toolkit.RoundingAuto)
			if int(vfloat) == vint && vint != 0 {
				dataType = DataInt
			} else if vfloat != 0 {
				// dataType = DataFloat
				b, i := toolkit.MemberIndex(DataFloats, dataType)
				if b {
					for idx, dataFloat := range DataFloats {
						if idx == i {
							dataType = dataFloat
						}
					}
				}
			} else {
				dataType = DataString
			}
		}
	}

	if dataType == DataDate {
		return toolkit.String2Date(data, dateFormat)
	} else if dataType == DataInt {
		return toolkit.ToInt(data, toolkit.RoundingAuto)
	} else if toolkit.HasMember(DataFloats, dataType) {
		return toolkit.ToFloat64(data, 2, toolkit.RoundingAuto)
	} else {
		return data
	}

	return nil
}
