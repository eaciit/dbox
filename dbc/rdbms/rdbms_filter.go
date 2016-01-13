package rdbms

import ( 
	"github.com/eaciit/dbox"
	"github.com/eaciit/cast" 
)

type FilterBuilder struct {
	dbox.FilterBuilder
}
func (fb *FilterBuilder) BuildFilter(f *dbox.Filter) (interface{}, error) {
	fm :="" 
	// vals := ""
	if f.Op == dbox.FilterOpEqual {
		fm = fm + f.Field + "= '"+cast.ToString(f.Value)+"'"
	} else if f.Op == dbox.FilterOpNoEqual {
		fm = fm + f.Field + "<>'"+cast.ToString(f.Value)+"'" 
	} else if f.Op == dbox.FilterOpGt {
		fm = fm + f.Field + " > '"+cast.ToString(f.Value)+"'" 
	} else if f.Op == dbox.FilterOpGte {
		fm = fm + f.Field + " >= '"+cast.ToString(f.Value)+"'" 
	} else if f.Op == dbox.FilterOpLt {
		fm = fm + f.Field + " < '"+cast.ToString(f.Value)+"'"  
	} else if f.Op == dbox.FilterOpLte {
		fm = fm + f.Field + " <= '"+cast.ToString(f.Value)+"'"   
	} else if f.Op == dbox.FilterOpOr || f.Op == dbox.FilterOpAnd {
		// bfs := []interface{}{}
		fs := f.Value.([]*dbox.Filter)
		for _, ff := range fs {
			bf, _ := fb.BuildFilter(ff)
			// if eb == nil {
			// 	bfs = append(bfs, bf)
			// }
			if fm == "" {
				fm = cast.ToString(bf)
			}else{
				if f.Op == dbox.FilterOpOr{
					fm = fm + " OR "+ cast.ToString(bf) 
				}else{
				    fm = fm + " AND "+ cast.ToString(bf) 
				}
			}
		}
		//fm.Set(f.Op, bfs)
	} else {
		//return nil, fmt.Errorf("Filter Op %s is not defined", f.Op)
	}
 
	return fm, nil
}

func (fb *FilterBuilder) CombineFilters(mfs []interface{}) (interface{}, error) {
	ret := ""
	if len(mfs) == 0 {
		return ret, nil
	}
	if len(mfs) == 1 {
		return mfs[0].(string), nil
	}
	for _, v := range mfs {
		vm := v.(string)
		if ret == "" {
			ret = vm
		}else{
			ret = ret + " AND " + vm
		}
	} 
	return ret, nil
}