package dbox

type IQuery interface {
	Cursor() (*Cursor, error)

	SetThis(IQuery) IQuery

	Select(...string) IQuery
	From(string) IQuery
	Where(...*Filter) IQuery
}

type Query struct {
	thisQuery IQuery
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
