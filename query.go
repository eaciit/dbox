package dbox

type QueryPartType string

const (
	QueryPart_Select  = "SELECT"
	QueryPart_From    = "FROM"
	QueryPart_Where   = "WHERE"
	QueryPart_Group   = "GROUP BY"
	QueryPart_Order   = "ORDER BY"
	QueryPart_Insert  = "INSERT"
	QueryPart_Update  = "UPDATE"
	QueryPart_Delete  = "DELETE"
	QueryPart_Save    = "SAVE"
	QueryPart_Command = "COMMAND"

	QueryPart_Join      = "JOIN"
	QueryPart_LeftJoin  = "LEFT JOIN"
	QueryPart_RightJoin = "RIGHT JOIN"
)

type IQuery interface {
	Cursor() (*Cursor, error)

	SetThis(IQuery) IQuery

	Select(...string) IQuery
	From(string) IQuery
	Where(...*Filter) IQuery
}

type QueryPart struct {
	PartType string
	Value    interface{}
}

type Query struct {
	thisQuery IQuery

	Parts []*QueryPart
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

func (q *Query) SetThis(t IQuery) IQuery {
	q.thisQuery = t
	return q
}

func (q *Query) Cursor() (*Cursor, error) {
	return nil, nil
}

func (q *Query) Select(ss ...string) IQuery {
	return q
}

func (q *Query) From(objname string) IQuery {
	return q
}

func (q *Query) Where(fs ...*Filter) IQuery {
	return q
}
