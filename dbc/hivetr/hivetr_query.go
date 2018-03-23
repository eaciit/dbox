package hivetr

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"github.com/kharism/dbox"
	"github.com/kharism/gohive"
)

type Query struct {
	SelectField    []string
	TableName      string
	Condition      string
	SortCondition  string
	GroupCondition string
	AggregateField []dbox.AggrInfo
	RawAggregate   string

	isSelect bool
	isInsert bool
	isUpdate bool
	isDelete bool
	SkipNum  int
	TakeNum  int

	RawQuery  string
	Conn      dbox.IConnection
	ConfigMap map[string]interface{}
	Closable  io.Closer
}

func (q *Query) Cursor(config toolkit.M) (dbox.ICursor, error) {
	resultSet, e := q.Conn.(*Connection).Conn.Query(q.String())
	if e != nil {
		return nil, e
	}
	_, e = resultSet.Wait()
	if e != nil {
		return nil, e
	}
	cursor := &Cursor{}
	cursor.Conn = q.Conn
	cursor.rowSet = resultSet.(*gohive.RowSetR)
	return cursor, nil
}
func (q *Query) String() string {
	output := ""

	if q.isSelect {
		output += "SELECT "

		if len(q.SelectField) == 0 && len(q.AggregateField) == 0 {
			output += "*"
		}
		if len(q.SelectField) > 0 {
			output += strings.Join(q.SelectField, " ,")
		}
		if len(q.AggregateField) > 0 {
			tempAggr := AggregateToString(q.AggregateField)
			if len(q.SelectField) > 0 {
				output += " ," + tempAggr
			} else {
				output += tempAggr
			}
		}
		output += " FROM " + q.TableName
		if q.Condition != "" {
			output += " WHERE " + q.Condition
		}
		if q.GroupCondition != "" {
			output += q.GroupCondition
		}
		if q.TakeNum > 0 || q.SkipNum > 0 {
			output += " LIMIT " + strconv.Itoa(q.SkipNum) + "," + strconv.Itoa(q.TakeNum) + " "
		}

	} else if q.isInsert {
		output += "INSERT INTO " + q.TableName + " VALUES "
	}

	return output
}
func (q *Query) Exec(toolkit.M) error {
	return errors.New(errorlib.NotYetImplemented)
}
func (q *Query) ExecOut(toolkit.M) (int64, error) {
	return 0, errors.New(errorlib.NotYetImplemented)
}

//-- getter
func (q *Query) Connection() dbox.IConnection {
	return q.Conn
}
func (q *Query) Config(key string, defaultVal interface{}) interface{} {
	val, ok := q.ConfigMap[key]
	if ok {
		return val
	}
	return defaultVal
}

//-- setter
func (q *Query) SetConnection(conn dbox.IConnection) dbox.IQuery {
	q.Conn = conn
	return q
}
func (q *Query) SetThis(aa dbox.IQuery) dbox.IQuery {
	return q
}
func (q *Query) SetConfig(key string, value interface{}) dbox.IQuery {
	if q.ConfigMap == nil {
		q.ConfigMap = map[string]interface{}{}
	}
	q.ConfigMap[key] = value
	return q
}

//-- pagination
func (q *Query) Take(take int) dbox.IQuery {
	q.TakeNum = take
	return q
}
func (q *Query) Skip(skip int) dbox.IQuery {
	q.SkipNum = skip
	return q
}

//-- chain
func (q *Query) Select(fields ...string) dbox.IQuery {
	q.isSelect = true
	q.SelectField = fields
	return q
}
func (q *Query) From(tableName string) dbox.IQuery {
	q.TableName = tableName
	return q
}
func (q *Query) Where(filter ...*dbox.Filter) dbox.IQuery {
	fs := []string{}
	for i, _ := range filter {
		fs = append(fs, FilterToString(filter[i]))
	}
	q.Condition = strings.Join(fs, " AND ")
	return q
}
func AggregateToString(infos []dbox.AggrInfo) string {
	fields := []string{}
	for _, aggr := range infos {
		newAggr := aggr.Op[1:] + "(" + aggr.Field.(string) + ")"
		if aggr.Alias != "" {
			newAggr += " AS " + aggr.Alias
		}
		fields = append(fields, newAggr)
	}
	output := strings.Join(fields, " , ")
	return "(" + output + ")"
}
func FilterToString(filter *dbox.Filter) string {
	output := ""
	P := ""
	if filter.Op != dbox.FilterOpAnd && filter.Op != dbox.FilterOpOr {
		if filter.Op == dbox.FilterOpEqual {
			P = "="
		} else if filter.Op == dbox.FilterOpLt {
			P = "<"
		} else if filter.Op == dbox.FilterOpGt {
			P = ">"
		} else if filter.Op == dbox.FilterOpLte {
			P = "<="
		} else if filter.Op == dbox.FilterOpGte {
			P = ">="
		} else if filter.Op == dbox.FilterOpNoEqual {
			P = "!="
		}

		if _, ok := filter.Value.(string); ok {
			output += fmt.Sprintf(" %s%s%q", filter.Field, P, filter.Value.(string)) //filter.Field + " \"" + P + "\" " + filter.Value.(string)
		} else if _, ok := filter.Value.(int); ok {
			pp := filter.Value.(int)
			output += filter.Field + " " + P + " " + strconv.Itoa(pp)
		} else if _, ok := filter.Value.(float64); ok {
			pp := filter.Value.(float64)
			output += filter.Field + " " + P + " " + fmt.Sprintf("%f", pp)
		}
	} else if filter.Op == dbox.FilterOpAnd || filter.Op == dbox.FilterOpOr {
		joiner := strings.ToUpper(filter.Op[1:])
		subResult := []string{}
		subFilters := filter.Value.([]*dbox.Filter)
		for _, k := range subFilters {
			subResult = append(subResult, FilterToString(k))
		}
		output2 := strings.Join(subResult, " "+joiner+" ")
		output += " (" + output2 + ") "
	}
	return output
}
func (q *Query) Order(order ...string) dbox.IQuery {
	q.SortCondition = strings.Join(order, ",")
	return q
}
func (q *Query) Group(group ...string) dbox.IQuery {
	q.GroupCondition = strings.Join(group, ",")
	return q
}

func (q *Query) Command(cmd string, aa interface{}) dbox.IQuery {
	return q
}
func (q *Query) Aggr(op string, field interface{}, alias string) dbox.IQuery {
	if q.AggregateField == nil {
		q.AggregateField = []dbox.AggrInfo{}
	}
	q.AggregateField = append(q.AggregateField, dbox.AggrInfo{op, field, alias})
	return q
}

//-- op
func (q *Query) Insert() dbox.IQuery {
	return q
}
func (q *Query) Save() dbox.IQuery {
	return q
}
func (q *Query) Update() dbox.IQuery {
	return q
}
func (q *Query) Delete() dbox.IQuery {
	return q
}

//-- other
func (q *Query) HasConfig(configname string) bool {
	return false
}
func (q *Query) Parts() []*dbox.QueryPart {
	return []*dbox.QueryPart{}
}
func (q *Query) AddPart(newPar *dbox.QueryPart) dbox.IQuery {
	return q
}
func (q *Query) Prepare() error {
	return nil
}
func (q *Query) Close() {
	if q.Closable != nil {
		q.Close()
	}
}
