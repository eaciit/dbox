package dbox

const (
	packageName   = "eaciit.dbox"
	modConnection = "Connection"
)

type DBOP string

const (
	DBINSERT  DBOP = "insert"
	DBUPDATE  DBOP = "update"
	DBDELETE  DBOP = "delete"
	DBSELECT  DBOP = "select"
	DBSAVE    DBOP = "save"
	DBCOMMAND DBOP = "command"
	DBUKNOWN  DBOP = "unknown"
)

func (d *DBOP) String() string {
	return string(*d)
}
