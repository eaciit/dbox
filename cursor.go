package dbox

type ICursor interface {
	Execute() error
	Close()
	Count() int
	ResetFetch() error
	Fetch(interface{}, int, bool) error
}

type Cursor struct {
}

func (c *Cursor) Execute() error {
	return nil
}

func (c *Cursor) Close() {
}

func (c *Cursor) Count() int {
	return 0
}

func (c *Cursor) ResetFetch() error {
	return nil
}

func (c *Cursor) Fetch(o interface{}, n int,
	closeWhenDone bool) error {
	return nil
}
