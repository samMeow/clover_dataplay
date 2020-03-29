package database

import (
	"data_play/pkg/parser"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type QueryerTestSuite struct {
	suite.Suite
	sqlxDB  *sqlx.DB
	mock    sqlmock.Sqlmock
	queryer Queryer
}

func (s *QueryerTestSuite) SetupSuite() {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		fmt.Println(err)
	}
	s.sqlxDB = sqlx.NewDb(mockDB, "sqlmock")
	s.mock = mock
	s.queryer = &QueryerImpl{}
}

func (s *QueryerTestSuite) TestCreateTableSuccess() {
	meta := []*parser.SQLMeta{
		&parser.SQLMeta{
			Name:     "name",
			Size:     10,
			DataType: "TEXT",
		},
		&parser.SQLMeta{
			Name:     "名前",
			Size:     300,
			DataType: "TEXT",
		},
		&parser.SQLMeta{
			Name:     "active",
			Size:     1,
			DataType: "BOOLEAN",
		},
		&parser.SQLMeta{
			Name:     "count",
			Size:     8,
			DataType: "INTEGER",
		},
		&parser.SQLMeta{
			Name:     "what",
			Size:     1,
			DataType: "UNKNOWN",
		},
	}

	s.mock.ExpectExec("^CREATE TABLE IF NOT EXISTS TestCreateTableSuccess .* name VARCHAR\\(10\\), 名前 TEXT, active BOOLEAN, count NUMERIC\\(8\\), what TEXT .*").WillReturnResult(sqlmock.NewResult(1, 1))
	err := s.queryer.CreateTable(s.sqlxDB, "TestCreateTableSuccess", meta)
	assert.Nil(s.T(), err)
}

func (s *QueryerTestSuite) TestCreateTableFail() {
	meta := []*parser.SQLMeta{}

	s.mock.ExpectExec("^CREATE TABLE IF NOT EXISTS TestCreateTableSuccess").WillReturnError(fmt.Errorf("sth wrong"))
	err := s.queryer.CreateTable(s.sqlxDB, "TestCreateTableSuccess", meta)
	assert.Error(s.T(), err)
}

func (s *QueryerTestSuite) TestInsertDataSingle() {
	data := []*map[string]interface{}{
		&map[string]interface{}{
			"名前":     "abc",
			"active": true,
			"count":  321,
		},
	}

	s.mock.ExpectExec(
		"^INSERT INTO TestInsertDataSingle \\(名前, active, count\\) VALUES \\(\\$1, \\$2, \\$3\\)",
	).WillReturnResult(sqlmock.NewResult(1, 1))
	err := s.queryer.InsertData(s.sqlxDB, "TestInsertDataSingle", data)
	assert.Nil(s.T(), err)
}

func (s *QueryerTestSuite) TestInsertDataMultiple() {
	data := []*map[string]interface{}{
		&map[string]interface{}{
			"名前":     "abc",
			"active": true,
			"count":  321,
		},
		&map[string]interface{}{
			"名前":     "世界",
			"active": false,
			"count":  123,
		},
	}

	s.mock.ExpectExec(
		"^INSERT INTO TestInsertDataSingle \\(名前, active, count\\) VALUES \\(\\$1, \\$2, \\$3\\), \\(\\$4, \\$5, \\$6\\)",
	).WithArgs("abc", true, 321, "世界", false, 123).WillReturnResult(sqlmock.NewResult(1, 1))
	err := s.queryer.InsertData(s.sqlxDB, "TestInsertDataSingle", data)
	assert.Nil(s.T(), err)
}

func (s *QueryerTestSuite) TestInsertDataFail() {
	data := []*map[string]interface{}{
		&map[string]interface{}{
			"名前":     "abc",
			"active": true,
			"count":  321,
		},
	}

	s.mock.ExpectExec(
		"^INSERT INTO TestInsertDataSingle",
	).WillReturnError(fmt.Errorf("whatever"))
	err := s.queryer.InsertData(s.sqlxDB, "TestInsertDataSingle", data)
	assert.Error(s.T(), err)
}

func (s *QueryerTestSuite) TearDownSuite() {
	s.sqlxDB.Close()
}

func TestQueryer(t *testing.T) {
	suite.Run(t, new(QueryerTestSuite))
}
