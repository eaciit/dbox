package dbox

const (
	packageName   = "eaciit.dbox"
	modConnection = "Connection"
)

type DB_OP string

const (
	DB_INSERT  DB_OP = "insert"
	DB_UPDATE  DB_OP = "update"
	DB_DELETE  DB_OP = "delete"
	DB_SELECT  DB_OP = "select"
	DB_SAVE    DB_OP = "save"
	DB_COMMAND DB_OP = "command"
	DB_UKNOWN  DB_OP = "unknown"
)

func (d *DB_OP) String() string {
	return string(*d)
}
