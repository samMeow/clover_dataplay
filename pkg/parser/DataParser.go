package parser

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

type DataParser struct {
	Metas []*SQLMeta
}

type DataScanner struct {
	Metas   []*SQLMeta
	file    *os.File
	Scanner *bufio.Scanner
}

func NewDataParser(metas []*SQLMeta) *DataParser {
	return &DataParser{
		Metas: metas,
	}
}

func (dp *DataParser) Parse(filePath string) (*DataScanner, error) {
	var file *os.File
	var err error
	file, err = os.Open(filePath)
	if err != nil {
		return nil, err
	}

	return &DataScanner{
		Metas:   dp.Metas,
		file:    file,
		Scanner: bufio.NewScanner(file),
	}, nil
}

func (ds *DataScanner) ReadRow() (*map[string]interface{}, bool, error) {
	var output = make(map[string]interface{})
	hasData := ds.Scanner.Scan()
	if !hasData {
		return nil, hasData, nil
	}
	if err := ds.Scanner.Err(); err != nil {
		return nil, false, err
	}

	row := ds.Scanner.Text()
	remain := []rune(row)
	for _, meta := range ds.Metas {
		raw := remain[0:meta.Size]
		datum, err := parseData(string(raw), meta.DataType)
		if err != nil {
			return nil, true, err
		}
		output[meta.Name] = datum
		remain = remain[meta.Size:]
	}
	return &output, true, nil
}

func (ds *DataScanner) Close() {
	ds.file.Close()
}

// assuming datatype only consist of INTEGER, BOOLEAN, TEXT
func parseData(datum, dataType string) (interface{}, error) {
	switch dataType {
	case "INTEGER":
		return strconv.Atoi(strings.TrimSpace(datum))
	case "BOOLEAN":
		return strconv.ParseBool(datum)
	case "TEXT":
	}
	return strings.TrimSpace(datum), nil
}
