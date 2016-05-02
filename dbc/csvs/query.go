package csvs

import (
	"encoding/csv"
	"fmt"
	"github.com/eaciit/cast"
	// "github.com/eaciit/crowd.old"
	"github.com/eaciit/crowd"
	"github.com/eaciit/dbox"
	err "github.com/eaciit/errorlib"
	"github.com/eaciit/toolkit"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
)

type headerstruct struct {
	name     string
	dataType string
}

type Query struct {
	dbox.Query
	sync.Mutex

	filePath string
	file     *os.File
	tempfile *os.File

	reader *csv.Reader
	writer *csv.Writer

	isUseHeader  bool
	headerColumn []headerstruct

	newfile   bool
	newheader bool

	fileHasBeenOpened bool
	indexes           []int
	newFileWrite      bool
	execOpr           bool
}

func (q *Query) Cursor(in toolkit.M) (dbox.ICursor, error) {
	var cursor *Cursor

	setting, e := q.prepare(in)
	if e != nil {
		return nil, err.Error(packageName, modQuery, "Cursor", e.Error())
	}

	if setting.GetString("commandtype") != dbox.QueryPartSelect {
		return nil, err.Error(packageName, modQuery, "Cursor", "Cursor is only working with select command, please use .Exec instead")
	}

	e = q.openFile()
	if e != nil {
		return nil, err.Error(packageName, modQuery, "Cursor", e.Error())
	}

	cursor = newCursor(q)
	where := setting.Get("where", []*dbox.Filter{}).([]*dbox.Filter)

	cursor.skip = setting.Get("skip", 0).(int)
	cursor.limit = setting.Get("take", 0).(int)
	if cursor.skip > 0 && cursor.limit > 0 {
		cursor.limit += cursor.skip
	}
	cursor.fields = setting.Get("fields", toolkit.M{}).(toolkit.M)

	// if len(where) > 0 {
	cursor.where = where
	cursor.indexes, e = q.generateIndex(where)
	if e != nil {
		return nil, err.Error(packageName, modQuery, "Cursor", e.Error())
	}
	// }

	return cursor, nil
}

func (q *Query) Exec(in toolkit.M) error {
	setting, e := q.prepare(in)
	commandType := setting["commandtype"].(string)

	if e != nil {
		return err.Error(packageName, modQuery, "Exec: "+commandType, e.Error())
	}

	if commandType == dbox.QueryPartSelect {
		return err.Error(packageName, modQuery, "Exec: "+commandType, "Exec is not working with select command, please use .Cursor instead")
	}

	q.Lock()
	defer q.Unlock()

	datam := toolkit.M{}

	if in.Has("data") {
		datam, e = toolkit.ToM(in["data"])
		if e != nil {
			return err.Error(packageName, modQuery, "Exec: "+commandType, e.Error())
		}
	}

	if commandType != dbox.QueryPartDelete && len(datam) == 0 {
		return err.Error(packageName, modQuery, "Exec: "+commandType, "Exec is not working, need data to param input")
	}
	// fmt.Println("DEBUG-113")
	e = q.openFile()
	if e != nil {
		return err.Error(packageName, modQuery, "Exec ", e.Error())
	}

	where := setting.Get("where", []*dbox.Filter{}).([]*dbox.Filter)
	if nameid := toolkit.IdField(datam); nameid != "" {
		if commandType == dbox.QueryPartInsert || commandType == dbox.QueryPartSave {
			where = make([]*dbox.Filter, 0, 0)
		}
		where = append(where, dbox.Eq(strings.ToLower(nameid), datam[nameid]))
	}

	q.indexes = make([]int, 0, 0)
	if len(where) > 0 {
		q.indexes, e = q.generateIndex(where)
		if e != nil {
			return err.Error(packageName, modQuery, "Exec: ", e.Error())
		}
	}

	if q.newheader && commandType != dbox.QueryPartSave && commandType != dbox.QueryPartInsert {
		return err.Error(packageName, modQuery, "Exec: ", "Only save and insert permited")
	}

	q.newFileWrite = false
	q.execOpr = false
	switch commandType {
	case dbox.QueryPartSave:
		if len(q.indexes) > 0 {
			q.newFileWrite = true
			e = q.execQueryPartUpdate(datam)
		} else {
			e = q.execQueryPartInsert(datam)
		}
	case dbox.QueryPartInsert:
		if len(q.indexes) > 0 {
			e = fmt.Errorf("Id found, cannot insert with same Id")
		} else {
			e = q.execQueryPartInsert(datam)
		}
	case dbox.QueryPartDelete:
		q.newFileWrite = true
		if len(q.indexes) > 0 {
			e = q.execQueryPartDelete()
		}
	case dbox.QueryPartUpdate:
		q.newFileWrite = true
		if len(q.indexes) > 0 {
			e = q.execQueryPartUpdate(datam)
		}
	}

	if e != nil {
		return err.Error(packageName, modQuery, "Exec: ", e.Error())
	}

	return nil
}

func (q *Query) Close() {
}

func (q *Query) openFile() error {
	q.newheader = false
	if q.fileHasBeenOpened {
		return nil
	}

	ci := q.Connection().(*Connection).Info()

	q.newfile = ci.Settings.Get("newfile", false).(bool)
	q.isUseHeader = ci.Settings.Get("useheader", false).(bool)

	_, e := os.Stat(q.filePath)
	// fmt.Printf("e : %v && q.newfile : %v && q.isUseHeader : %v \n\n", os.IsNotExist(e), q.newfile, q.isUseHeader)
	if os.IsNotExist(e) && q.newfile && q.isUseHeader {
		_, ex := os.Stat(q.Connection().(*Connection).folder)
		if ex == nil {
			q.newheader = true
			_, e = os.Create(q.filePath)
		}
	}
	// fmt.Println("DEBUG-197 : ", q.newheader)
	if e != nil && (strings.Contains(e.Error(), "does not exist") || strings.Contains(e.Error(), "no such file or directory")) {
		return err.Error(packageName, modQuery, "file doesn't exist", e.Error())
	} else if e != nil {
		return err.Error(packageName, modQuery, "openFile: Open file fail", e.Error())
	}

	q.file, e = os.Open(q.filePath)
	if e != nil {
		return err.Error(packageName, modQuery, "openFile: Read file data fail", e.Error())
	}

	q.reader = csv.NewReader(q.file)
	q.setReaderParam()
	q.setConfigParam()

	q.fileHasBeenOpened = true
	return nil
}

func (q *Query) setReaderParam() {
	ci := q.Connection().(*Connection).Info()

	if delimiter := ci.Settings.Get("delimiter", "").(string); delimiter != "" {
		q.reader.Comma = rune(delimiter[0])
	}

	if comment := ci.Settings.Get("comment", "").(string); comment != "" {
		q.reader.Comment = rune(comment[0])
	}

	if fieldsPerRecord := ci.Settings.GetInt("fieldsperrecord"); fieldsPerRecord > 0 {
		q.reader.FieldsPerRecord = fieldsPerRecord
	}

	if lazyQuotes := ci.Settings.Get("lazyquotes", false).(bool); lazyQuotes {
		q.reader.LazyQuotes = lazyQuotes
	}

	if trailingComma := ci.Settings.Get("trailingcomma", false).(bool); trailingComma {
		q.reader.TrailingComma = trailingComma
	}

	if trimLeadingSpace := ci.Settings.Get("trimleadingspace", false).(bool); trimLeadingSpace {
		q.reader.TrailingComma = trimLeadingSpace
	}
}

func (q *Query) setConfigParam() {
	ci := q.Connection().(*Connection).Info()

	q.newfile = ci.Settings.Get("newfile", false).(bool)
	q.isUseHeader = ci.Settings.Get("useheader", false).(bool)

	// set header from reader =============== }
	dateformat := ci.Settings.Get("dateformat", "").(string)
	q.headerColumn = make([]headerstruct, 0, 0)

	tdread, e := q.reader.Read()
	for i, v := range tdread {
		ts := headerstruct{}
		ts.name = string(i)
		ts.dataType = ""
		if q.isUseHeader {
			ts.name = v
		}
		q.headerColumn = append(q.headerColumn, ts)
	}

	if q.isUseHeader && e == nil {
		tdread, e = q.reader.Read()
	}

	isCheckType := true
	ix := 0
	for isCheckType && e != io.EOF {
		isCheckType = false

		for i, v := range tdread {
			if v != "" {
				matchNumber := false
				matchFloat := false
				matchDate := false

				formatDate := "((^(0[0-9]|[0-9]|(1|2)[0-9]|3[0-1])(\\.|\\/|-)(0[0-9]|[0-9]|1[0-2])(\\.|\\/|-)[\\d]{4}$)|(^[\\d]{4}(\\.|\\/|-)(0[0-9]|[0-9]|1[0-2])(\\.|\\/|-)(0[0-9]|[0-9]|(1|2)[0-9]|3[0-1])$))"
				matchDate, _ = regexp.MatchString(formatDate, v)
				if !matchDate && dateformat != "" {
					d := cast.String2Date(v, dateformat)
					if d.Year() > 1 {
						matchDate = true
					}
				}

				x := strings.Index(v, ".")

				if x > 0 {
					matchFloat = true
					v = strings.Replace(v, ".", "", 1)
				}

				matchNumber, _ = regexp.MatchString("^\\d+$", v)

				q.headerColumn[i].dataType = "string"
				if matchNumber {
					q.headerColumn[i].dataType = "int"
					if matchFloat {
						q.headerColumn[i].dataType = "float"
					}
				}

				if matchDate {
					q.headerColumn[i].dataType = "date"
				}
			}
		}

		for _, v := range q.headerColumn {
			if v.dataType == "" {
				isCheckType = true
			}
		}

		if isCheckType {
			tdread, e = q.reader.Read()
		}

		ix++
		if ix > 10 {
			break
		}
	}

	for _, v := range q.headerColumn {
		if v.dataType == "" {
			v.dataType = "string"
		}
	}

	_ = q.resetReader()
	// ===================== }

	if ci.Settings.Has("mapheader") {
		smh := ci.Settings["mapheader"].([]toolkit.M)
		for i, val := range smh {
			ts := headerstruct{}
			for name, dt := range val {
				ts.name = name
				ts.dataType = cast.ToString(dt)
			}
			if (i + 1) < len(q.headerColumn) {
				q.headerColumn[i] = ts
			} else {
				q.headerColumn = append(q.headerColumn, ts)
			}
		}
	}
}

func (q *Query) resetReader() error {
	// ci := q.Connection().(*Connection).Info()
	var e error
	if q.file != nil {
		q.file.Close()
	}
	q.file, e = os.Open(q.filePath)
	if e != nil {
		return err.Error(packageName, modQuery, "open file for reset fail", e.Error())
	}
	q.reader = csv.NewReader(q.file)
	q.setReaderParam()
	// fmt.Printf("q.isUseHeader : %v && !q.newheader : %v\n\n", q.isUseHeader, q.newheader)
	if q.isUseHeader && !q.newheader {
		_, e = q.reader.Read()
		if e != nil {
			return err.Error(packageName, modQuery, "read csv for reset fail", e.Error())
		}
	}
	return nil
}

func (q *Query) generateIndex(filters []*dbox.Filter) (output []int, e error) {
	var n int = 0
	for {
		tdread, e := q.reader.Read()
		if e != nil && e != io.EOF {
			break
		}
		n++

		tm := toolkit.M{}

		for i, v := range tdread {
			tolower := strings.ToLower(q.headerColumn[i].name)
			tm.Set(tolower, v)
			if q.headerColumn[i].dataType == "int" {
				tm[tolower] = cast.ToInt(v, cast.RoundingAuto)
			} else if q.headerColumn[i].dataType == "float" {
				tm[tolower] = cast.ToF64(v, (len(v) - (strings.IndexAny(v, "."))), cast.RoundingAuto)
			}
		}

		match := dbox.MatchM(tm, filters)
		if (len(filters) == 0 || match) && len(tm) > 0 {
			output = append(output, n)
		}

		if e == io.EOF {
			break
		}
	}

	e = q.resetReader()
	return
}

// func (q *Query) writeFile() error {
// 	_, e := os.Stat(q.filePath)
// 	if e != nil && e == os.ErrNotExist {
// 		f, e := os.Create(q.filePath)
// 		if e != nil {
// 			return err.Error(packageName, modQuery, "writeFile", e.Error())
// 		}
// 		f.Close()
// 	}

// 	bs := toolkit.Jsonify(q.data)
// 	e = ioutil.WriteFile(q.filePath, bs, 0644)
// 	if e != nil {
// 		return err.Error(packageName, modQuery, "WriteFile", e.Error())
// 	}
// 	return nil
// }

func (q *Query) prepare(in toolkit.M) (output toolkit.M, e error) {
	output = toolkit.M{}
	quyerParts := q.Parts()
	c := crowd.From(&quyerParts)
	groupParts := c.Group(func(x interface{}) interface{} {
		return x.(*dbox.QueryPart).PartType
	}, nil).Exec()
	parts := map[interface{}]interface{}{}
	if len(groupParts.Result.Data().([]crowd.KV)) > 0 {
		for _, kv := range groupParts.Result.Data().([]crowd.KV) {
			parts[kv.Key] = kv.Value
		}
	}

	fromParts, hasFrom := parts[dbox.QueryPartFrom]
	if hasFrom == false {
		return nil, err.Error(packageName, "Query", "prepare", "Invalid table name")
	}
	tablename := fromParts.([]*dbox.QueryPart)[0].Value.(string)
	output.Set("tablename", tablename)
	q.filePath = filepath.Join(q.Connection().(*Connection).folder, tablename+".csv")

	skip := 0
	if skipParts, hasSkip := parts[dbox.QueryPartSkip]; hasSkip {
		skip = skipParts.([]*dbox.QueryPart)[0].Value.(int)
	}
	output.Set("skip", skip)

	take := 0
	if takeParts, has := parts[dbox.QueryPartTake]; has {
		take = takeParts.([]*dbox.QueryPart)[0].Value.(int)
	}
	output.Set("take", take)

	var aggregate bool
	aggrParts, hasAggr := parts[dbox.QueryPartAggr]
	aggrExpression := toolkit.M{}
	if hasAggr {
		aggregate = true
		aggrElements := func() []*dbox.QueryPart {
			var qps []*dbox.QueryPart
			for _, v := range aggrParts.([]*dbox.QueryPart) {
				qps = append(qps, v)
			}
			return qps
		}()
		for _, el := range aggrElements {
			aggr := el.Value.(dbox.AggrInfo)
			aggrExpression.Set(aggr.Alias, toolkit.M{}.Set(aggr.Op, aggr.Field))
		}
	}

	partGroup, hasGroup := parts[dbox.QueryPartGroup]
	if hasGroup {
		aggregate = true
		groups := func() toolkit.M {
			s := toolkit.M{}
			for _, v := range partGroup.([]*dbox.QueryPart) {
				gs := v.Value.([]string)
				for _, g := range gs {
					if strings.TrimSpace(g) != "" {
						s.Set(g, "$"+g)
					}
				}
			}
			return s
		}()
		if len(groups) == 0 {
			aggrExpression.Set("_id", "")
		} else {
			aggrExpression.Set("_id", groups)
		}
	}

	output.Set("aggregate", aggregate)
	output.Set("aggrExpression", aggrExpression)

	var fields toolkit.M
	selectParts, hasSelect := parts[dbox.QueryPartSelect]
	if hasSelect {
		fields = toolkit.M{}
		for _, sl := range selectParts.([]*dbox.QueryPart) {
			for _, fid := range sl.Value.([]string) {
				fields.Set(fid, 1)
			}
		}
		output.Set("commandtype", dbox.QueryPartSelect)
	} else {
		_, hasUpdate := parts[dbox.QueryPartUpdate]
		_, hasInsert := parts[dbox.QueryPartInsert]
		_, hasDelete := parts[dbox.QueryPartDelete]
		_, hasSave := parts[dbox.QueryPartSave]

		if hasInsert {
			output.Set("commandtype", dbox.QueryPartInsert)
		} else if hasUpdate {
			output.Set("commandtype", dbox.QueryPartUpdate)
		} else if hasDelete {
			output.Set("commandtype", dbox.QueryPartDelete)
		} else if hasSave {
			output.Set("commandtype", dbox.QueryPartSave)
		} else {
			output.Set("commandtype", dbox.QueryPartSelect)
		}
	}
	output.Set("fields", fields)

	var sort []string
	sortParts, hasSort := parts[dbox.QueryPartOrder]
	if hasSort {
		sort = []string{}
		for _, sl := range sortParts.([]*dbox.QueryPart) {
			for _, fid := range sl.Value.([]string) {
				sort = append(sort, fid)
			}
		}
	}
	output.Set("sort", sort)

	var filters []*dbox.Filter
	whereParts, hasWhere := parts[dbox.QueryPartWhere]
	if hasWhere {
		for _, p := range whereParts.([]*dbox.QueryPart) {
			fs := p.Value.([]*dbox.Filter)
			for _, f := range fs {
				f = ReadVariable(f, in)
				filters = append(filters, f)
			}
		}
	}
	output.Set("where", filters)
	return
}

func (q *Query) startWriteMode() error {
	var e error
	ci := q.Connection().(*Connection).Info()
	if q.newFileWrite {
		q.tempfile, e = os.OpenFile(q.filePath+".temp", os.O_CREATE, 0666)
	} else {
		q.tempfile, e = os.OpenFile(q.filePath, os.O_RDWR|os.O_APPEND, 0666)
	}

	if e != nil {
		return e
	}

	q.writer = csv.NewWriter(q.tempfile)

	if delimiter := ci.Settings.Get("delimiter", "").(string); delimiter != "" {
		q.writer.Comma = rune(delimiter[0])
	}

	if q.newFileWrite && q.isUseHeader && len(q.headerColumn) > 0 {
		datatemp := make([]string, 0)
		for _, v := range q.headerColumn {
			datatemp = append(datatemp, v.name)
		}

		q.writer.Write(datatemp)
		q.writer.Flush()
	}

	return nil
}

func (q *Query) endWriteMode() error {
	var e error
	q.writer = nil
	e = q.file.Close()
	if e != nil {
		return e
	}

	if q.newFileWrite {
		e = q.tempfile.Close()
		if e != nil {
			return e
		}
		if q.execOpr {
			e = os.Remove(q.filePath)
			if e != nil {
				return e
			} else {
				e = os.Rename(q.filePath+".temp", q.filePath)
			}
		} else {
			e = os.Remove(q.filePath + ".temp")
		}
	}

	if e != nil {
		return e
	}

	return nil
}

// func (q *Query) execQueryPartSave(dt toolkit.M) error {
// 	if len(dt) == 0 {
// 		return errorlib.Error(packageName, modQuery, "save", "data to insert is not found")
// 	}

// 	writer := q.writer
// 	reader := q.reader
// 	tempHeader := []string{}

// 	for _, val := range q.headerColumn {
// 		tempHeader = append(tempHeader, val.name)
// 	}

// 	// Check ID Before Insert
// 	checkidfound := false
// 	if nameid := toolkit.IdField(dt); nameid != "" {
// 		q.updatessave = true

// 		var colsid int
// 		for i, val := range q.headerColumn {
// 			if val.name == nameid {
// 				colsid = i
// 			}
// 		}

// 		for {
// 			dataTempSearch, e := reader.Read()
// 			for i, val := range dataTempSearch {
// 				if i == colsid && val == dt[nameid] {
// 					checkidfound = true
// 					break
// 				}
// 			}
// 			if e == io.EOF {
// 				break
// 			} else if e != nil {
// 				return errorlib.Error(packageName, modQuery, "Save", e.Error())
// 			}
// 		}
// 	}

// 	if checkidfound {
// 		e := q.EndSessionWrite()
// 		if e != nil {
// 			return errorlib.Error(packageName, modQuery, "Save", e.Error())
// 		}

// 		q.TypeOpenFile = TypeOpenFile_Create

// 		e = q.StartSessionWrite()
// 		if e != nil {
// 			return errorlib.Error(packageName, modQuery, "Save", e.Error())
// 		}

// 		e = q.execQueryPartUpdate(dt, QueryCondition{})
// 		if e != nil {
// 			return errorlib.Error(packageName, modQuery, "Save", e.Error())
// 		}
// 		// time.Sleep(1000 * time.Millisecond)
// 	} else {
// 		//Change to Do Insert
// 		dataTemp := []string{}

// 		for _, v := range q.headerColumn {
// 			if dt.Has(v.name) {
// 				dataTemp = append(dataTemp, cast.ToString(dt[v.name]))
// 			} else {
// 				dataTemp = append(dataTemp, "")
// 			}
// 		}

// 		if len(dataTemp) > 0 {
// 			writer.Write(dataTemp)
// 			writer.Flush()
// 		}
// 	}

// 	return nil
// }

func (q *Query) setNewHeader(dt toolkit.M) {
	q.headerColumn = make([]headerstruct, 0, 0)
	sliceheader := make([]string, 0, 0)

	// tdread, e := q.reader.Read()
	for key, _ := range dt {
		ts := headerstruct{}
		ts.name = key
		ts.dataType = ""
		q.headerColumn = append(q.headerColumn, ts)
		sliceheader = append(sliceheader, key)
	}

	q.writer.Write(sliceheader)
	q.writer.Flush()
	q.newheader = false

	return
}

func (q *Query) execQueryPartInsert(dt toolkit.M) error {
	var e error
	e = q.startWriteMode()

	if e != nil {
		return err.Error(packageName, modQuery, "Exec-Insert: ", e.Error())
	}

	if q.newheader {
		q.setNewHeader(dt)
	}

	writer := q.writer
	// reader := q.reader
	dataTemp := []string{}

	for _, v := range q.headerColumn {
		if dt.Has(v.name) {
			dataTemp = append(dataTemp, cast.ToString(dt[v.name]))
		} else {
			dataTemp = append(dataTemp, "")
		}
	}

	if len(dataTemp) > 0 {
		writer.Write(dataTemp)
		writer.Flush()
	}

	q.execOpr = true
	e = q.endWriteMode()
	if e != nil {
		return err.Error(packageName, modQuery, "Exec-Insert: ", e.Error())
	}

	return nil
}

func (q *Query) execQueryPartDelete() error {
	var e error
	e = q.startWriteMode()
	if e != nil {
		return err.Error(packageName, modQuery, "Exec-Delete: ", e.Error())
	}

	writer := q.writer
	reader := q.reader
	tempHeader := []string{}

	for _, val := range q.headerColumn {
		tempHeader = append(tempHeader, val.name)
	}

	var i int = 0
	for {
		i += 1

		dataTemp, e := reader.Read()
		if e == io.EOF {
			if !toolkit.HasMember(q.indexes, i) && len(dataTemp) > 0 {
				writer.Write(dataTemp)
				writer.Flush()
			}
			break
		} else if e != nil {
			_ = q.endWriteMode()
			return err.Error(packageName, modQuery, "Exec-Delete: ", e.Error())
		}

		if !toolkit.HasMember(q.indexes, i) && len(dataTemp) > 0 {
			writer.Write(dataTemp)
			writer.Flush()
		}
	}

	q.execOpr = true
	e = q.endWriteMode()
	if e != nil {
		return err.Error(packageName, modQuery, "Exec-Delete: ", e.Error())
	}
	return nil

}

func (q *Query) execQueryPartUpdate(dt toolkit.M) error {
	var e error
	e = q.startWriteMode()
	if e != nil {
		return err.Error(packageName, modQuery, "Exec-Update: ", e.Error())
	}

	writer := q.writer
	reader := q.reader
	tempHeader := []string{}

	for _, val := range q.headerColumn {
		tempHeader = append(tempHeader, val.name)
	}

	var i int = 0

	for {
		i += 1

		dataTemp, e := reader.Read()
		if toolkit.HasMember(q.indexes, i) && len(dataTemp) > 0 {
			for n, v := range tempHeader {
				if dt.Has(v) {
					dataTemp[n] = cast.ToString(dt[v])
				}
			}
		}

		if e == io.EOF {
			if len(dataTemp) > 0 {
				writer.Write(dataTemp)
				writer.Flush()
			}
			break
		} else if e != nil {
			_ = q.endWriteMode()
			return err.Error(packageName, modQuery, "Exec-Update:", e.Error())
		}
		if len(dataTemp) > 0 {
			writer.Write(dataTemp)
			writer.Flush()
		}
	}

	q.execOpr = true
	e = q.endWriteMode()
	if e != nil {
		return err.Error(packageName, modQuery, "Exec-Update: ", e.Error())
	}
	return nil
}

func ReadVariable(f *dbox.Filter, in toolkit.M) *dbox.Filter {
	f.Field = strings.ToLower(f.Field)
	if (f.Op == "$and" || f.Op == "$or") &&
		strings.Contains(reflect.TypeOf(f.Value).String(), "dbox.Filter") {
		fs := f.Value.([]*dbox.Filter)
		/* nilai fs :  [0xc082059590 0xc0820595c0]*/
		for i, ff := range fs {
			/* nilai ff[0] : &{umur $gt @age} && ff[1] : &{name $eq @nama}*/
			bf := ReadVariable(ff, in)
			/* nilai bf[0] :  &{umur $gt 25} && bf[1] : &{name $eq Kane}*/
			fs[i] = bf
		}
		f.Value = fs
		return f
	} else {
		if reflect.TypeOf(f.Value).Kind() == reflect.Slice {
			if strings.Contains(reflect.TypeOf(f.Value).String(), "interface") {
				fSlice := f.Value.([]interface{})
				/*nilai fSlice : [@name1 @name2]*/
				for i := 0; i < len(fSlice); i++ {
					/* nilai fSlice [i] : @name1*/
					if string(cast.ToString(fSlice[i])[0]) == "@" {
						for key, val := range in {
							if cast.ToString(fSlice[i]) == key {
								fSlice[i] = val
							}
						}
					}
				}
				f.Value = fSlice
			} else if strings.Contains(reflect.TypeOf(f.Value).String(), "string") {
				fSlice := f.Value.([]string)
				for i := 0; i < len(fSlice); i++ {
					if string(fSlice[i][0]) == "@" {
						for key, val := range in {
							if fSlice[i] == key {
								fSlice[i] = val.(string)
							}
						}
					}
				}
				f.Value = fSlice
			}
			return f
		} else {
			if string(cast.ToString(f.Value)[0]) == "@" {
				for key, val := range in {
					if cast.ToString(f.Value) == key {
						f.Value = val
					}
				}
			}
			return f
		}
	}
	return f
}
