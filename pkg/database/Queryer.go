package database

import (
	"data_play/pkg/parser"
	"fmt"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
)

type Queryer interface {
	CreateTable(conn sqlx.Execer, tableName string, metas []*parser.SQLMeta) error
	InsertData(conn sqlx.Ext, tableName string, rows []*map[string]interface{}) error
}

type QueryerImpl struct{}

func (q *QueryerImpl) CreateTable(conn sqlx.Execer, tableName string, metas []*parser.SQLMeta) error {
	sqlTmpl := `CREATE TABLE IF NOT EXISTS %s (
		%s
	)`
	var rows []string
	for _, meta := range metas {
		rows = append(rows, createRowStmt(meta))
	}
	sql := fmt.Sprintf(sqlTmpl, tableName, strings.Join(rows, ",\n"))
	_, err := conn.Exec(sql)
	return err
}

func createRowStmt(meta *parser.SQLMeta) string {
	switch meta.DataType {
	case "INTEGER":
		return fmt.Sprintf("%s NUMERIC(%d)", meta.Name, meta.Size)
	case "BOOLEAN":
		return fmt.Sprintf("%s BOOLEAN", meta.Name)
	case "TEXT":
		if meta.Size < 256 {
			return fmt.Sprintf("%s VARCHAR(%d)", meta.Name, meta.Size)
		}
	}
	return fmt.Sprintf("%s TEXT", meta.Name)
}

func intRange(min, max int) []int {
	a := make([]int, max-min+1)
	for i := range a {
		a[i] = min + i
	}
	return a
}
func toPlaceHolder(arr []int) string {
	str := make([]string, len(arr))
	for i, v := range arr {
		str[i] = "$" + strconv.Itoa(v)
	}
	return "(" + strings.Join(str, ", ") + ")"
}

func (q *QueryerImpl) InsertData(conn sqlx.Ext, tableName string, rows []*map[string]interface{}) error {
	sqlTmpl := `INSERT INTO %s (%s) VALUES %s;`
	var columns []string
	if len(rows) == 0 {
		return nil
	}
	for k := range *rows[0] {
		columns = append(columns, k)
	}
	var placeholders []string
	var values []interface{}
	for i, row := range rows {
		pos := i * len(columns)
		holder := toPlaceHolder(intRange(pos+1, pos+len(columns)))
		placeholders = append(placeholders, holder)
		var val []interface{}
		for _, k := range columns {
			val = append(val, (*row)[k])
		}
		values = append(values, val...)
	}

	sql := fmt.Sprintf(sqlTmpl, tableName, strings.Join(columns, ", "), strings.Join(placeholders, ", "))
	_, err := conn.Exec(sql, values...)
	return err
}
