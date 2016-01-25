package dbox

import (
	"github.com/eaciit/cast"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"regexp"
	"strings"
)

type IConnection interface {
	Connect() error
	Close()

	Info() *ConnectionInfo
	SetInfo(*ConnectionInfo)

	NewQuery() IQuery

	Fb() IFilterBuilder
	SetFb(IFilterBuilder)
}

type FnNewConnection func(*ConnectionInfo) (IConnection, error)

var connectors map[string]FnNewConnection

func RegisterConnector(connector string, fn FnNewConnection) {
	if connectors == nil {
		connectors = map[string]FnNewConnection{}
	}
	connectors[connector] = fn
}

func NewConnection(connector string, ci *ConnectionInfo) (IConnection, error) {
	if connectors == nil {
		return nil, errorlib.Error(packageName, "", "NewConnection", "Invalid connector")
	}

	fn, found := connectors[connector]
	if found == false {
		return nil, errorlib.Error(packageName, "", "NewConnection", "Invalid connector")
	}
	return fn(ci)
}

type ConnectionInfo struct {
	Host     string
	Database string
	UserName string
	Password string

	Settings toolkit.M
}

type Connection struct {
	info *ConnectionInfo
	fb   IFilterBuilder
}

func (c *Connection) Connect() error {
	return errorlib.Error(packageName, modConnection,
		"Connect", errorlib.NotYetImplemented)
}

func (c *Connection) Info() *ConnectionInfo {
	return c.info
}

func (c *Connection) SetInfo(i *ConnectionInfo) {
	c.info = i
}

func (c *Connection) SetFb(fb IFilterBuilder) {
	c.fb = fb
}

func (c *Connection) Fb() IFilterBuilder {
	if c.fb == nil {
		c.fb = new(FilterBuilder)
	}
	return c.fb
}

func (c *Connection) Close() {
}

func (c *Connection) NewQuery() IQuery {
	q := new(Query)
	return q
}

func NewQueryFromSQL(c IConnection, qstr string) (IQuery, error) {
	q := c.NewQuery()
	// qstr = strings.ToLower(qstr)
	r := regexp.MustCompile(`(([Ss][Ee][Ll][Ee][Cc][Tt])) (?P<select>([^\-\.~!@#$%\^&()=+\-\[\]{}\\:;<>?/]*)) (([Ff][Rr][Oo][Mm])) (?P<from>([a-zA-Z][_a-zA-Z]+[_a-zA-Z0-1]*)) (([Ww][Hh][Ee][Rr][Ee])) (?P<where>(.*))`)

	if !(r.MatchString(qstr)) {
		r = regexp.MustCompile(`(([Ss][Ee][Ll][Ee][Cc][Tt])) (?P<select>([^\-\.~!@#$%\^&()=+\-\[\]{}\\:;<>?/]*)) (([Ff][Rr][Oo][Mm])) (?P<from>([a-zA-Z][_a-zA-Z]+[_a-zA-Z0-1]*))`)
	}

	if !(r.MatchString(qstr)) {
		return nil, errorlib.Error(packageName, "", "NewQueryFromSQL", "Invalid query format")
	}

	temparray := r.FindStringSubmatch(qstr)
	sqlpart := toolkit.M{}

	for i, val := range r.SubexpNames() {
		if val != "" {
			sqlpart.Set(val, temparray[i])
		}
	}

	// strselect := r.FindStringSubmatch(qstr)[3]
	// strfrom := r.FindStringSubmatch(qstr)[7]
	// strwhere := ""
	// if isUseWhere {
	// 	strwhere = r.FindStringSubmatch(qstr)[11]
	// }

	partselect := strings.Split(sqlpart.Get("select", "").(string), ",")
	for i, val := range partselect {
		partselect[i] = strings.TrimSpace(val)
	}

	partfrom := strings.TrimSpace(sqlpart.Get("from", "").(string))

	partwhere := new(Filter)
	if sqlpart.Has("where") {
		partwhere = generateFilterQuerySQL(sqlpart.Get("where", "").(string))
	}

	// partorder := make([]string, 0, 0)
	// partgroup := make([]string, 0, 0)

	if len(partselect) > 0 {
		q.Select(partselect...)
	}

	if partfrom != "" {
		q.From(partfrom)
	}

	if sqlpart.Has("where") {
		q.Where(partwhere)
	}

	return q, nil
}

func generateFilterQuerySQL(strwhere string) (fb *Filter) {
	strwhere = strings.TrimSpace(strwhere)

	r := regexp.MustCompile(`(?P<currcond>(.*)) (?P<oprandor>([Aa][Nn][Dd]|[Oo][Rr])) (?P<nextcond>(.*))`)
	if !r.MatchString(strwhere) {
		if cond, _ := regexp.MatchString(`^\(.*\)$`, strwhere); cond {
			strwhere = strings.TrimSuffix(strings.TrimPrefix(strwhere, "("), ")")
		}
		r = regexp.MustCompile(`(?P<field>([a-zA-Z][_a-zA-Z]+[_a-zA-Z0-1]*))(?P<opr>((\s)*(=|<>|[><](=)?)|(\s(I|i)(N|n)\s)|(\s(L|l)(I|i)(K|k)(E|e)\s)))(?P<value>(.*))`)
	}

	temparray := r.FindStringSubmatch(strwhere)
	condpart := toolkit.M{}

	for i, val := range r.SubexpNames() {
		if val != "" {
			condpart.Set(val, temparray[i])
		}
	}

	if condpart.Has("oprandor") {
		// arfilter := []*Filter{}
		//Check Bracket if regexp.MatchString(`^(.*)\(.*([Aa][Nn][Dd]|[Oo][Rr]).*\)(.*)$`, strwhere) {
		if strings.TrimSpace(condpart.Get("oprandor", "").(string)) == "and" {
			fb = And(generateFilterQuerySQL(condpart.Get("currcond", "").(string)), generateFilterQuerySQL(condpart.Get("nextcond", "").(string)))
		} else {
			fb = Or(generateFilterQuerySQL(condpart.Get("currcond", "").(string)), generateFilterQuerySQL(condpart.Get("nextcond", "").(string)))
		}
	} else {
		var (
			asv []interface{}
			iv  interface{}
		)

		c1 := strings.TrimSpace(condpart.Get("field", "").(string))
		tv := strings.TrimSpace(condpart.Get("value", "").(string))
		opr := strings.ToLower(strings.TrimSpace(condpart.Get("opr", "").(string)))

		if opr != "in" {
			if strings.Contains(tv, `'`) || strings.Contains(tv, `"`) {
				sv := strings.Replace(strings.Replace(tv, `'`, "", -1), `"`, "", -1)
				if opr == "like" {
					sv = strings.Replace(strings.Replace(sv, "%", "(.)*", -1), "_", "(.)?", -1)
				}
				iv = sv
			} else if strings.Contains(tv, `.`) {
				iv = cast.ToF64(tv, (len(tv) - (strings.IndexAny(tv, "."))), cast.RoundingAuto)
			} else {
				iv = cast.ToInt(tv, cast.RoundingAuto)
			}
		} else {
			tv = strings.Replace(strings.Replace(tv, "(", "", 1), ")", "", 1)
			// asv = strings.Split(tv, ",")
			for _, val := range strings.Split(tv, ",") {
				var tiv interface{}
				ttv := strings.TrimSpace(val)
				if strings.Contains(ttv, `'`) || strings.Contains(ttv, `"`) {
					tiv = strings.Replace(strings.Replace(ttv, `'`, "", -1), `"`, "", -1)
				} else if strings.Contains(ttv, `.`) {
					tiv = cast.ToF64(ttv, (len(ttv) - (strings.IndexAny(ttv, "."))), cast.RoundingAuto)
				} else {
					tiv = cast.ToInt(ttv, cast.RoundingAuto)
				}
				asv = append(asv, tiv)
			}
		}

		switch opr {
		case "=":
			fb = Eq(c1, iv)
		case "<>":
			fb = Ne(c1, iv)
		case ">":
			fb = Lt(c1, iv)
		case "<":
			fb = Gt(c1, iv)
		case ">=":
			fb = Lte(c1, iv)
		case "<=":
			fb = Gte(c1, iv)
		case "like":
			fb = Contains(c1, iv.(string))
		case "in":
			fb = In(c1, asv...)
		}
	}

	return
}
