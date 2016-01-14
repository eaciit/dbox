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
	Exec(toolkit.M) error

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

	Command(string, interface{}) IQuery
	Aggr(string, interface{}, string) IQuery

	//-- op
	Insert() IQuery
	Save() IQuery
	Update() IQuery
	Delete() IQuery

	//-- other
	HasConfig(string) bool
	Parts() []*QueryPart
	AddPart(*QueryPart) IQuery
	Prepare() error
	Close()
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

func (q *Query) initParts() {
	if q.parts == nil {
		q.parts = []*QueryPart{}
	}
}

func populateParmValue(inputM *toolkit.M, parms toolkit.M) {
	in := *inputM
	for k, _ := range in {
		if parms.Has(k) {
			in[k] = parms[k]
		} else {
			in[k] = ""
		}
	}
	*inputM = in
}

func (q *Query) Command(commandName string, m interface{}) IQuery {
	//q.initParts()
	qp := new(QueryPart)
	qp.PartType = commandName
	qp.Value = m
	//q.parts = append(q.parts, qp)
	return q.AddPart(qp)
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

func (q *Query) Prepare() error {
	return errorlib.Error(packageName, modQuery, "Cursor",
		errorlib.NotYetImplemented)
}

func (q *Query) Exec(parm toolkit.M) error {
	return errorlib.Error(packageName, modQuery, "Exec", errorlib.NotYetImplemented)
}

func (q *Query) Close() {
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
func (q *Query) Insert() IQuery {
	//q.AddPart(&QueryPart{QueryPartData, obj})
	q.AddPart(&QueryPart{QueryPartInsert, nil})
	return q.this()
}
func (q *Query) Save() IQuery {
	//q.AddPart(&QueryPart{QueryPartData, obj})
	q.AddPart(&QueryPart{QueryPartSave, nil})
	return q.this()
}

func (q *Query) Update() IQuery {
	//q.AddPart(&QueryPart{QueryPartData, obj})
	q.AddPart(&QueryPart{QueryPartUpdate, nil})
	return q.this()
}

func (q *Query) Delete() IQuery {
	//q.AddPart(&QueryPart{QueryPartDelete, nil})
	q.AddPart(&QueryPart{QueryPartDelete, nil})
	return q.this()
}
