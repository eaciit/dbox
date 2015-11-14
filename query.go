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

	QueryPartTake = "TAKE"
	QueryPartSkip = "SKIP"

	QueryPartJoin      = "JOIN"
	QueryPartLeftJoin  = "LEFT JOIN"
	QueryPartRightJoin = "RIGHT JOIN"
	QueryPartData      = "DATA"
	QueryPartParm      = "PARM"

	QueryConfigPooling = "pooling"
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

	//-- op
	Insert(interface{}, toolkit.M) IQuery
	Save(interface{}, toolkit.M) IQuery
	Update(interface{}, toolkit.M) IQuery
	Delete(toolkit.M) IQuery

	//-- other
	HasConfig(string) bool
}

type QueryPart struct {
	PartType string
	Value    interface{}
}

type Query struct {
	thisQuery IQuery
	conn      IConnection

	Parts  []*QueryPart
	config toolkit.M
}

func (q *Query) this() IQuery {
	if q.thisQuery == nil {
		return q
	} else {
		return q.thisQuery
	}
}

func (q *Query) addPart(qp *QueryPart) IQuery {
	if q.Parts == nil {
		q.Parts = []*QueryPart{}
	}
	q.Parts = append(q.Parts, qp)
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
	q.addPart(&QueryPart{QueryPartSelect, ss})
	return q.this()
}

func (q *Query) From(objname string) IQuery {
	q.addPart(&QueryPart{QueryPartFrom, objname})
	return q.this()
}

func (q *Query) Where(fs ...*Filter) IQuery {
	q.addPart(&QueryPart{QueryPartWhere, fs})
	return q.this()
}

func (q *Query) Order(ords ...string) IQuery {
	q.addPart(&QueryPart{QueryPartOrder, ords})
	return q.this()
}

func (q *Query) Group(groups ...string) IQuery {
	q.addPart(&QueryPart{QueryPartGroup, groups})
	return q.this()
}

func (q *Query) Take(i int) IQuery {
	q.addPart(&QueryPart{QueryPartTake, i})
	return q.this()
}
func (q *Query) Skip(i int) IQuery {
	q.addPart(&QueryPart{QueryPartSkip, i})
	return q.this()
}
func (q *Query) Insert(obj interface{}, in toolkit.M) IQuery {
	q.addPart(&QueryPart{QueryPartData, obj})
	q.addPart(&QueryPart{QueryPartInsert, in})
	return q.this()
}
func (q *Query) Save(obj interface{}, in toolkit.M) IQuery {
	q.addPart(&QueryPart{QueryPartData, obj})
	q.addPart(&QueryPart{QueryPartSave, in})
	return q.this()
}

func (q *Query) Update(obj interface{}, in toolkit.M) IQuery {
	q.addPart(&QueryPart{QueryPartData, obj})
	q.addPart(&QueryPart{QueryPartUpdate, in})
	return q.this()
}

func (q *Query) Delete(in toolkit.M) IQuery {
	q.addPart(&QueryPart{QueryPartParm, in})
	return q.this()
}
