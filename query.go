package dbox

import (
	"github.com/eaciit/toolkit"

	"github.com/eaciit/errorlib"
)

type QueryPartType string

const (
	modQuery = "Query"

	QueryPartSelect  = "SELECT"
	QueryPartFrom    = "FROM"
	QueryPartWhere   = "WHERE"
	QueryPartGroup   = "GROUP BY"
	QueryPartOrder   = "ORDER BY"
	QueryPartInsert  = "INSERT"
	QueryPartUpdate  = "UPDATE"
	QueryPartDelete  = "DELETE"
	QueryPartSave    = "SAVE"
	QueryPartCommand = "COMMAND"
	QueryPartAggr    = "AGGR"
	QueryPartCustom  = "CUSTOM"

	QueryPartTake = "TAKE"
	QueryPartSkip = "SKIP"

	QueryPartJoin      = "JOIN"
	QueryPartLeftJoin  = "LEFT JOIN"
	QueryPartRightJoin = "RIGHT JOIN"
	QueryPartData      = "DATA"
	QueryPartParm      = "PARM"

	QueryConfigPooling = "pooling"

	AggrSum  = "$sum"
	AggrAvr  = "$avg"
	AggrMin  = "$min"
	AggrMax  = "$max"
	AggrMean = "$mean"
	AggrMed  = "$med"
)

type IQuery interface {
	//-- ouputs
	Cursor(toolkit.M) (ICursor, error)
	Exec(interface{}, toolkit.M) error

	//-- getter
	Connection() IConnection
	Config(string, interface{}) interface{}

	//-- setter
	SetConnection(IConnection) IQuery
	SetThis(IQuery) IQuery
	SetConfig(string, interface{}) IQuery

	//-- pagination
	Take(int) IQuery
	Skip(int) IQuery

	//-- chain
	Select(...string) IQuery
	From(string) IQuery
	Where(...*Filter) IQuery
	Order(...string) IQuery
	Group(...string) IQuery

	Aggr(string, interface{}, string) IQuery

	//-- op
	Insert(interface{}, toolkit.M) IQuery
	Save(interface{}, toolkit.M) IQuery
	Update(interface{}, toolkit.M) IQuery
	Delete(toolkit.M) IQuery

	//-- other
	HasConfig(string) bool
	Parts() []*QueryPart
	AddPart(*QueryPart) IQuery
}

type AggrInfo struct {
	Op    string
	Field interface{}
	Alias string
}

type QueryPart struct {
	PartType string
	Value    interface{}
}

type Query struct {
	thisQuery IQuery
	conn      IConnection

	parts  []*QueryPart
	config toolkit.M
}

func (q *Query) this() IQuery {
	if q.thisQuery == nil {
		return q
	} else {
		return q.thisQuery
	}
}

func (q *Query) Parts() []*QueryPart {
	if q.parts == nil {
		q.parts = []*QueryPart{}
	}
	return q.parts
}

func (q *Query) AddPart(qp *QueryPart) IQuery {
	if q.parts == nil {
		q.parts = []*QueryPart{}
	}
	q.parts = append(q.parts, qp)
	return q.this()
}

func (q *Query) SetConnection(c IConnection) IQuery {
	q.conn = c
	return q.this()
}

func (q *Query) SetThis(t IQuery) IQuery {
	q.thisQuery = t
	return t
}
func (q *Query) Connection() IConnection {
	return q.conn
}
func (q *Query) Config(k string, def interface{}) interface{} {
	if q.config == nil {
		return def
	}
	return q.config.Get(k, def)
}
func (q *Query) HasConfig(k string) bool {
	if q.config == nil {
		return false
	}
	return q.config.Has(k)
}
func (q *Query) SetConfig(k string, v interface{}) IQuery {
	if q.config == nil {
		q.config = toolkit.M{}
	}
	q.config.Set(k, v)
	return q.this()
}

func (q *Query) Cursor(in toolkit.M) (ICursor, error) {
	return nil,
		errorlib.Error(packageName, modQuery, "Cursor",
			errorlib.NotYetImplemented)
}

func (q *Query) Exec(result interface{}, in toolkit.M) error {
	return nil
}

func (q *Query) Select(ss ...string) IQuery {
	q.AddPart(&QueryPart{QueryPartSelect, ss})
	return q.this()
}

func (q *Query) From(objname string) IQuery {
	q.AddPart(&QueryPart{QueryPartFrom, objname})
	return q.this()
}

func (q *Query) Where(fs ...*Filter) IQuery {
	q.AddPart(&QueryPart{QueryPartWhere, fs})
	return q.this()
}

func (q *Query) Aggr(op string, field interface{}, alias string) IQuery {
	q.AddPart(&QueryPart{QueryPartAggr, AggrInfo{op, field, alias}})
	return q.this()
}

func (q *Query) Order(ords ...string) IQuery {
	q.AddPart(&QueryPart{QueryPartOrder, ords})
	return q.this()
}

func (q *Query) Group(groups ...string) IQuery {
	q.AddPart(&QueryPart{QueryPartGroup, groups})
	return q.this()
}

func (q *Query) Take(i int) IQuery {
	q.AddPart(&QueryPart{QueryPartTake, i})
	return q.this()
}
func (q *Query) Skip(i int) IQuery {
	q.AddPart(&QueryPart{QueryPartSkip, i})
	return q.this()
}
func (q *Query) Insert(obj interface{}, in toolkit.M) IQuery {
	q.AddPart(&QueryPart{QueryPartData, obj})
	q.AddPart(&QueryPart{QueryPartInsert, in})
	return q.this()
}
func (q *Query) Save(obj interface{}, in toolkit.M) IQuery {
	q.AddPart(&QueryPart{QueryPartData, obj})
	q.AddPart(&QueryPart{QueryPartSave, in})
	return q.this()
}

func (q *Query) Update(obj interface{}, in toolkit.M) IQuery {
	q.AddPart(&QueryPart{QueryPartData, obj})
	q.AddPart(&QueryPart{QueryPartUpdate, in})
	return q.this()
}

func (q *Query) Delete(in toolkit.M) IQuery {
	q.AddPart(&QueryPart{QueryPartParm, in})
	return q.this()
}
