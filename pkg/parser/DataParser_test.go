package parser

import (
	"bufio"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type DataParserTestSuite struct {
	suite.Suite
	Parser DataParser
}

func (s *DataParserTestSuite) SetupSuite() {
	s.Parser = NewDataParser([]*SQLMeta{})
}

func (s *DataParserTestSuite) TestParseNotExistsFile() {
	_, err := s.Parser.Parse("not exists")
	assert.Error(s.T(), err)
}

func (s *DataParserTestSuite) TestParseSuccess() {
	currentDir, _ := os.Getwd()
	scanner, err := s.Parser.Parse(currentDir + "/DataParser.go")
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), scanner)
}

type DataScannerTestSuite struct {
	suite.Suite
	Meta []*SQLMeta
}

func (s *DataScannerTestSuite) SetupSuite() {
	s.Meta = []*SQLMeta{
		&SQLMeta{
			Name:     "name",
			Size:     10,
			DataType: "TEXT",
		},
		&SQLMeta{
			Name:     "active",
			Size:     1,
			DataType: "BOOLEAN",
		},
		&SQLMeta{
			Name:     "count",
			Size:     5,
			DataType: "INTEGER",
		},
	}
}

func (s *DataScannerTestSuite) TestReadRowSuccess() {
	var datum = `Hello     1  123`
	scanner := &DataScanner{
		Metas:   s.Meta,
		Scanner: bufio.NewScanner(strings.NewReader(datum)),
	}
	row, haveData, err := scanner.ReadRow()
	assert.Nil(s.T(), err)
	assert.True(s.T(), haveData)
	assert.Equal(s.T(), *row, map[string]interface{}{
		"name":   "Hello",
		"active": true,
		"count":  123,
	})
}

func (s *DataScannerTestSuite) TestReadRowSuccessUTF8() {
	var datum = `アイウエオ     1  123`
	scanner := &DataScanner{
		Metas:   s.Meta,
		Scanner: bufio.NewScanner(strings.NewReader(datum)),
	}
	row, haveData, err := scanner.ReadRow()
	assert.Nil(s.T(), err)
	assert.True(s.T(), haveData)
	assert.Equal(s.T(), *row, map[string]interface{}{
		"name":   "アイウエオ",
		"active": true,
		"count":  123,
	})
}

func (s *DataScannerTestSuite) TestReadRowCanReadTwoRow() {
	var datum = `Hello     1  123
ABC       1 4321`
	scanner := &DataScanner{
		Metas:   s.Meta,
		Scanner: bufio.NewScanner(strings.NewReader(datum)),
	}
	scanner.ReadRow()
	row, haveData, err := scanner.ReadRow()
	assert.Nil(s.T(), err)
	assert.True(s.T(), haveData)
	assert.Equal(s.T(), *row, map[string]interface{}{
		"name":   "ABC",
		"active": true,
		"count":  4321,
	})
}

func (s *DataScannerTestSuite) TestReadRowShouldReachEof() {
	var datum = `Hello     1  123`
	scanner := &DataScanner{
		Metas:   s.Meta,
		Scanner: bufio.NewScanner(strings.NewReader(datum)),
	}
	scanner.ReadRow()
	row, haveData, err := scanner.ReadRow()
	assert.Nil(s.T(), err)
	assert.False(s.T(), haveData)
	assert.Nil(s.T(), row)
}

func (s *DataScannerTestSuite) TestReadRowShouldReturnError() {
	var datum = `Hello     1  1a3`
	scanner := &DataScanner{
		Metas:   s.Meta,
		Scanner: bufio.NewScanner(strings.NewReader(datum)),
	}
	row, _, err := scanner.ReadRow()
	assert.Error(s.T(), err)
	assert.Nil(s.T(), row)
}

func (s *DataScannerTestSuite) TestReadRowShouldReturnErrorWhenLengthShort() {
	var datum = `Hello`
	scanner := &DataScanner{
		Metas:   s.Meta,
		Scanner: bufio.NewScanner(strings.NewReader(datum)),
	}
	row, _, err := scanner.ReadRow()
	assert.Error(s.T(), err)
	assert.Nil(s.T(), row)
}

func TestDataParser(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(DataParserTestSuite))
	suite.Run(t, new(DataScannerTestSuite))
}
