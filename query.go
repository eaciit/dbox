package dbox

type IQuery interface {
	Cursor() *Cursor
}

type Query struct {
}

func (q *Query) Cursor() *Cursor {
	return nil
}
