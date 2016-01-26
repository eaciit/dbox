package dbox

import (
	"github.com/eaciit/cast"
	"github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"regexp"
	"strings"
)

type ObjTypeEnum string

const (
	ObjTypeTable     ObjTypeEnum = "table"
	ObjTypeView      ObjTypeEnum = "view"
	ObjTypeProcedure ObjTypeEnum = "procedure"
	ObjTypeAll       ObjTypeEnum = "allobject"
)

type IConnection interface {
	Connect() error
	Close()

	Info() *ConnectionInfo
	SetInfo(*ConnectionInfo)

	NewQuery() IQuery

	Fb() IFilterBuilder
	SetFb(IFilterBuilder)

	ObjectNames(ObjTypeEnum) []string
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

func (c *Connection) ObjectNames(obj ObjTypeEnum) []string {
	if obj == "" {
		obj = ObjTypeAll
	}
	return []string{}
}

func NewQueryFromSQL(c IConnection, qstr string) (IQuery, error) {
	q := c.NewQuery()
	var r *regexp.Regexp

	if cond, _ := regexp.MatchString(`^\([Ss][Ee][Ll][Ee][Cc][Tt]) (.*)$`, qstr); !cond {
		r = regexp.MustCompile(`(([Ss][Ee][Ll][Ee][Cc][Tt])) (?P<select>([^\-\.~!@#$%\^&()=+\-\[\]{}\\:;<>?/]*)) (([Ff][Rr][Oo][Mm])) (?P<from>([a-zA-Z][_a-zA-Z]+[_a-zA-Z0-1]*)) (([Ww][Hh][Ee][Rr][Ee])) (?P<where>(.*))`)
		if !(r.MatchString(qstr)) {
			r = regexp.MustCompile(`(([Ss][Ee][Ll][Ee][Cc][Tt])) (?P<select>([^\-\.~!@#$%\^&()=+\-\[\]{}\\:;<>?/]*)) (([Ff][Rr][Oo][Mm])) (?P<from>([a-zA-Z][_a-zA-Z]+[_a-zA-Z0-1]*))`)
		}
		if !(r.MatchString(qstr)) {
			return nil, errorlib.Error(packageName, "", "NewQueryFromSQL", "Invalid query format")
		}
	} else if cond, _ := regexp.MatchString(`^\([Ii][Nn][Ss][Ee][Rr][Tt]\s[Ii][Nn][Tt][Oo]) (.*)$`, qstr); !cond {
		r = regexp.MustCompile(`(?P<insert>([Ii][Nn][Ss][Ee][Rr][Tt]\s[Ii][Nn][Tt][Oo])) (?P<from>([a-zA-Z][_a-zA-Z]+[_a-zA-Z0-1]*)) (.*)`)
		if !(r.MatchString(qstr)) {
			return nil, errorlib.Error(packageName, "", "NewQueryFromSQL", "Invalid query format")
		}
	} else if cond, _ := regexp.MatchString(`^\([Uu][Pp][Dd][Aa][Tt][Ee]) (.*)$`, qstr); !cond {
		r = regexp.MustCompile(`(?P<update>([Uu][Pp][Dd][Aa][Tt][Ee])) (?P<from>([a-zA-Z][_a-zA-Z]+[_a-zA-Z0-1]*)) (.*) (([Ww][Hh][Ee][Rr][Ee])) (?P<where>(.*))`)
		if !(r.MatchString(qstr)) {
			r = regexp.MustCompile(`(?P<update>([Uu][Pp][Dd][Aa][Tt][Ee])) (?P<from>([a-zA-Z][_a-zA-Z]+[_a-zA-Z0-1]*)) (.*)`)
		}
		if !(r.MatchString(qstr)) {
			return nil, errorlib.Error(packageName, "", "NewQueryFromSQL", "Invalid query format")
		}
	} else if cond, _ := regexp.MatchString(`^\([Dd][Ee][Ll][Ee][Tt][Ee]) (.*)$`, qstr); !cond {
		r = regexp.MustCompile(`(?P<delete>([Dd][Ee][Ll][Ee][Tt][Ee])) (([Ff][Rr][Oo][Mm])) (?P<from>([a-zA-Z][_a-zA-Z]+[_a-zA-Z0-1]*)) (([Ww][Hh][Ee][Rr][Ee])) (?P<where>(.*))`)
		if !(r.MatchString(qstr)) {
			r = regexp.MustCompile(`(?P<delete>([Dd][Ee][Ll][Ee][Tt][Ee])) (([Ff][Rr][Oo][Mm])) (?P<from>([a-zA-Z][_a-zA-Z]+[_a-zA-Z0-1]*))`)
		}
		if !(r.MatchString(qstr)) {
			return nil, errorlib.Error(packageName, "", "NewQueryFromSQL", "Invalid query format")
		}
	} else {
		return nil, errorlib.Error(packageName, "", "NewQueryFromSQL", "Invalid query format")
	}

	temparray := r.FindStringSubmatch(qstr)
	sqlpart := toolkit.M{}

	for i, val := range r.SubexpNames() {
		if val != "" {
			sqlpart.Set(val, temparray[i])
		}
	}

	partselect := []string{}

	if sqlpart.Has("select") {
		partselect = strings.Split(sqlpart.Get("select", "").(string), ",")
		for i, val := range partselect {
			partselect[i] = strings.TrimSpace(val)
		}

		// partorder := make([]string, 0, 0)
		// partgroup := make([]string, 0, 0)
	} else if sqlpart.Has("insert") {
		q.Insert()
	} else if sqlpart.Has("update") {
		q.Update()
	} else if sqlpart.Has("delete") {
		q.Delete()
	}

	if len(partselect) > 0 {
		q.Select(partselect...)
	}

	if sqlpart.Has("from") {
		q.From(strings.TrimSpace(sqlpart.Get("from", "").(string)))
	}

	if sqlpart.Has("where") {
		q.Where(generateFilterQuerySQL(sqlpart.Get("where", "").(string)))
	}

	return q, nil
}

func generateFilterQuerySQL(strwhere string) (fb *Filter) {
	strwhere = strings.TrimSpace(strwhere)
	rbracket := regexp.MustCompile(`^\(.*\)$`)
	if rbracket.MatchString(strwhere) {
		if cond, _ := regexp.MatchString(`^\(.*\) ([Oo][Rr]|[Aa][Nn][Dd]) (.*)$`, strwhere); !cond {
			strwhere = strings.TrimSuffix(strings.TrimPrefix(strwhere, "("), ")")
		}
	}
	// toolkit.Printf("Connection 161 : %#v\n", strwhere)
	r := regexp.MustCompile(`^(?P<lastprocess01>(.*))(?P<firstprocess>(\(.*([Aa][Nn][Dd]|[Oo][Rr]).*\)))(?P<lastprocess02>(.*))$`)
	if !r.MatchString(strwhere) {
		r = regexp.MustCompile(`(.*) (?P<oprandor>([Oo][Rr])) (.*)`)
	}

	if !r.MatchString(strwhere) {
		r = regexp.MustCompile(`(.*) (?P<oprandor>([Aa][Nn][Dd])) (.*)`)
	}

	if !r.MatchString(strwhere) {
		r = regexp.MustCompile(`(?P<field>([a-zA-Z][_a-zA-Z]+[_a-zA-Z0-1]*))(?P<opr>((\s)*(=|<>|[><](=)?)|(\s(I|i)(N|n)\s)|(\s(L|l)(I|i)(K|k)(E|e)\s)))(?P<value>(.*))`)
	}

	temparray := r.FindStringSubmatch(strwhere)
	condpart := toolkit.M{}

	for i, val := range r.SubexpNames() {
		if val != "" {
			condpart.Set(val, temparray[i])
		}
	}

	arstrwhere := make([]string, 0, 0)
	oprstr := "or"
	if condpart.Has("firstprocess") {
		oprstr, arstrwhere = generateWhereCondition(strwhere)
		// for _, val := range generateWhereCondition(strwhere) {
		// 	arstrwhere = append(arstrwhere, val)
		// }

		// next01 := condpart.Get("lastprocess01", "").(string)
		// next02 := condpart.Get("lastprocess01", "").(string)
		// if condition {

		// }
		// //parsing check operator to and insert to arstwhere
	} else if condpart.Has("oprandor") {
		arstrwhere = strings.Split(strwhere, condpart["oprandor"].(string))
		if strings.ToLower(condpart["oprandor"].(string)) == "and" {
			oprstr = "and"
		}
	}

	if len(arstrwhere) > 0 {
		var arfilter []*Filter

		for _, swhere := range arstrwhere {
			arfilter = append(arfilter, generateFilterQuerySQL(swhere))
		}

		if oprstr == "and" {
			fb = And(arfilter...)
		} else {
			fb = Or(arfilter...)
		}
		// //Check Bracket if regexp.MatchString(`^(.*)\(.*([Aa][Nn][Dd]|[Oo][Rr]).*\)(.*)$`, strwhere) {
		// if strings.TrimSpace(condpart.Get("oprandor", "").(string)) == "and" {
		// 	fb = And(generateFilterQuerySQL(condpart.Get("currcond", "").(string)), generateFilterQuerySQL(condpart.Get("nextcond", "").(string)))
		// } else {
		// 	fb = Or(generateFilterQuerySQL(condpart.Get("currcond", "").(string)), generateFilterQuerySQL(condpart.Get("nextcond", "").(string)))
		// }
	} else {
		var (
			asv []interface{}
			iv  interface{}
		)

		c1 := strings.TrimSpace(condpart.Get("field", "").(string))
		tv := strings.TrimSpace(condpart.Get("value", "").(string))
		opr := strings.ToLower(strings.TrimSpace(condpart.Get("opr", "").(string)))
		// if condition {
		tv = strings.TrimSuffix(tv, ")")
		// }

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

func generateWhereCondition(strwhere string) (oprstr string, sstr []string) {

	oprstr = "or"
	r := regexp.MustCompile(`^(?P<lastprocess01>(.*))(?P<firstprocess>(\(.*([Aa][Nn][Dd]|[Oo][Rr]).*\)))(?P<lastprocess02>(.*))$`)
	// ror := regexp.MustCompile(`(.*) (?P<oprandor>([Oo][Rr])) (.*)`)
	// rand := regexp.MustCompile(`(.*) (?P<oprandor>([Aa][Nn][Dd])) (.*)`)
	// if !r.MatchString(strwhere) {
	// 	r = ror
	// }

	// if !r.MatchString(strwhere) {
	// 	r = rand
	// }

	// tempalias := make(toolkit.M, 0, 0)
	tempalias := toolkit.M{}
	for r.MatchString(strwhere) {
		condpart := toolkit.M{}
		temparray := r.FindStringSubmatch(strwhere)

		for i, val := range r.SubexpNames() {
			if val != "" && temparray[i] != "" {
				condpart.Set(val, temparray[i])
			}
		}

		straliaskey := "@" + toolkit.GenerateRandomString("1234567890", 10)
		strwhere = strings.Replace(strwhere, condpart["firstprocess"].(string), straliaskey, -1)
		tempalias.Set(straliaskey, condpart["firstprocess"])
	}

	r = regexp.MustCompile(`(.*) (?P<oprandor>([Oo][Rr])) (.*)`)
	if !r.MatchString(strwhere) {
		r = regexp.MustCompile(`(.*) (?P<oprandor>([Aa][Nn][Dd])) (.*)`)
	}

	condpart := toolkit.M{}
	temparray := r.FindStringSubmatch(strwhere)
	// toolkit.Printf("Connection 319 : %#v\n", temparray)
	for i, val := range r.SubexpNames() {
		if val != "" && temparray[i] != "" {
			condpart.Set(val, temparray[i])
		}
	}

	sstr = strings.Split(strwhere, condpart["oprandor"].(string))
	if strings.ToLower(condpart["oprandor"].(string)) == "and" {
		oprstr = "and"
	}

	for key, val := range tempalias {
		for i, strval := range sstr {
			sstr[i] = strings.Replace(strval, key, val.(string), -1)
		}
	}

	return
}
