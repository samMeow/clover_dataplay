package parser

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
)

type SQLMetaCSVParser struct {
	filePath string
	buffer   []byte
}

type SQLMeta struct {
	Name     string
	Size     int
	DataType string
}

func NewSQLMetaCSVParser(filePath string) (*SQLMetaCSVParser, error) {
	buffer, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	return &SQLMetaCSVParser{
		filePath: filePath,
		buffer:   buffer,
	}, nil
}

func (p *SQLMetaCSVParser) Parse() ([]*SQLMeta, error) {
	var err error
	var output []*SQLMeta

	lines := strings.Split(string(p.buffer), "\n")
	for i, line := range lines[1:] {
		if len(line) == 0 {
			continue
		}
		tokens := strings.Split(line, ",")
		if len(tokens) != 3 {
			return nil, fmt.Errorf("Fail to parse %s in line %d", p.filePath, i)
		}
		var size int
		size, err = strconv.Atoi(tokens[1])
		if err != nil {
			return nil, fmt.Errorf("Fail to parse %s in line %d", p.filePath, i)
		}
		output = append(output, &SQLMeta{
			Name:     tokens[0],
			Size:     size,
			DataType: tokens[2],
		})
	}
	return output, nil
}
