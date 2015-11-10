package DB_OP

type ICursor interface {
	Execute() error
	Close()
}

type Cursor struct {
}

func (c *Cursor) Execute() error {
	return nil
}

func (c *Cursor) Close() {
}
