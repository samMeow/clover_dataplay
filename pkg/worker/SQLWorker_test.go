package worker

import (
	"bufio"
	"context"
	"data_play/pkg/parser"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type MockQueryer struct {
	mock.Mock
}

func (q *MockQueryer) CreateTable(conn sqlx.Execer, tableName string, metas []*parser.SQLMeta) error {
	args := q.Called(conn, tableName, metas)
	return args.Error(0)
}

func (q *MockQueryer) InsertData(conn sqlx.Ext, tableName string, rows []*map[string]interface{}) error {
	args := q.Called(conn, tableName, rows)
	return args.Error(0)
}

type MockParserFactory struct {
	mock.Mock
}

func (pf *MockParserFactory) MakeParser(modelName string) (parser.DataParser, error) {
	args := pf.Called(modelName)
	return args.Get(0).(parser.DataParser), args.Error(1)
}

type MockDataParser struct {
	mock.Mock
}

func (dp *MockDataParser) Parse(filePath string) (*parser.DataScanner, error) {
	args := dp.Called(filePath)
	return args.Get(0).(*parser.DataScanner), args.Error(1)
}

func (dp *MockDataParser) Meta() []*parser.SQLMeta {
	args := dp.Called()
	return args.Get(0).([]*parser.SQLMeta)
}

type SQLWorkerTestSuite struct {
	suite.Suite
	parserFactory *MockParserFactory
	db            *sqlx.DB
	queryer       *MockQueryer
	mockDB        sqlmock.Sqlmock
	meta          []*parser.SQLMeta
}

func (s *SQLWorkerTestSuite) SetupSuite() {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		fmt.Println(err)
	}
	s.db = sqlx.NewDb(mockDB, "sqlmock")
	s.mockDB = mock
	s.meta = []*parser.SQLMeta{
		&parser.SQLMeta{
			Name:     "name",
			Size:     10,
			DataType: "TEXT",
		},
		&parser.SQLMeta{
			Name:     "active",
			Size:     1,
			DataType: "BOOLEAN",
		},
		&parser.SQLMeta{
			Name:     "count",
			Size:     5,
			DataType: "INTEGER",
		},
	}
}

func (s *SQLWorkerTestSuite) SetupTest() {
	s.queryer = new(MockQueryer)
	s.parserFactory = new(MockParserFactory)
}

func (s *SQLWorkerTestSuite) TearDownSuite() {
	s.db.Close()
}

func (s *SQLWorkerTestSuite) TestSafeInsertDataSuccess() {
	data := []*map[string]interface{}{}
	worker := &SQLWorker{
		DB:            s.db,
		Queryer:       s.queryer,
		ParserFactory: s.parserFactory,
		BufferSize:    500,
	}
	s.mockDB.ExpectBegin()
	s.queryer.On("InsertData", mock.Anything, "TestSafeInsertDataSuccess", data).Return(nil)
	s.mockDB.ExpectCommit()

	err := worker.safeInsertData(context.Background(), "TestSafeInsertDataSuccess", data)
	assert.Nil(s.T(), err)
}

func (s *SQLWorkerTestSuite) TestSafeInsertDataInsertFail() {
	data := []*map[string]interface{}{}
	worker := &SQLWorker{
		DB:            s.db,
		Queryer:       s.queryer,
		ParserFactory: s.parserFactory,
		BufferSize:    500,
	}
	s.queryer.On("InsertData", mock.Anything, "TestSafeInsertDataInsertFail", data).Return(fmt.Errorf("wrong"))

	s.mockDB.ExpectBegin()
	s.mockDB.ExpectRollback()
	err := worker.safeInsertData(context.Background(), "TestSafeInsertDataInsertFail", data)
	assert.Error(s.T(), err)
}

func (s *SQLWorkerTestSuite) TestRunInputJobSuccess() {
	worker := &SQLWorker{
		DB:            s.db,
		Queryer:       s.queryer,
		ParserFactory: s.parserFactory,
		BufferSize:    500,
	}
	dp := new(MockDataParser)
	s.parserFactory.On("MakeParser", "TestRunInputJobSuccess").Return(dp, nil)
	dp.On("Meta").Return(s.meta)
	dp.On("Parse", "TestRunInputJobSuccess_2020-03-29.txt").Return(&parser.DataScanner{
		Metas:   s.meta,
		Scanner: bufio.NewScanner(strings.NewReader(`Hello     1  123`)),
	}, nil)
	s.queryer.On("CreateTable", mock.Anything, "TestRunInputJobSuccess", mock.Anything).Return(nil)
	s.queryer.On(
		"InsertData",
		mock.Anything,
		"TestRunInputJobSuccess",
		[]*map[string]interface{}{
			&map[string]interface{}{
				"name":   "Hello",
				"active": true,
				"count":  123,
			},
		},
	).Return(nil)
	s.mockDB.ExpectBegin()
	s.mockDB.ExpectCommit()

	err := worker.runInputJob(context.Background(), "TestRunInputJobSuccess_2020-03-29.txt")
	assert.Nil(s.T(), err)
}

func (s *SQLWorkerTestSuite) TestRunInputJobFailParse() {
	worker := &SQLWorker{
		DB:            s.db,
		Queryer:       s.queryer,
		ParserFactory: s.parserFactory,
		BufferSize:    1,
	}
	dp := new(MockDataParser)
	s.parserFactory.On("MakeParser", "TestRunInputJobSuccess").Return(dp, nil)
	dp.On("Meta").Return(s.meta)
	dp.On("Parse", "TestRunInputJobSuccess_2020-03-29.txt").Return(&parser.DataScanner{
		Metas: s.meta,
		Scanner: bufio.NewScanner(strings.NewReader(`Hello     1  123
abc1123`)),
	}, nil)
	s.queryer.On("CreateTable", mock.Anything, "TestRunInputJobSuccess", mock.Anything).Return(nil)
	s.queryer.On(
		"InsertData",
		mock.Anything,
		"TestRunInputJobSuccess",
		[]*map[string]interface{}{
			&map[string]interface{}{
				"name":   "Hello",
				"active": true,
				"count":  123,
			},
		},
	).Return(nil)
	s.mockDB.ExpectBegin()
	s.mockDB.ExpectCommit()

	err := worker.runInputJob(context.Background(), "TestRunInputJobSuccess_2020-03-29.txt")
	assert.Error(s.T(), err)
}

func (s *SQLWorkerTestSuite) TestRunInputJobCancelledAtBegin() {
	cancelContext, cancel := context.WithCancel(context.Background())
	worker := &SQLWorker{
		DB:            s.db,
		Queryer:       s.queryer,
		ParserFactory: s.parserFactory,
		BufferSize:    1,
	}
	dp := new(MockDataParser)
	s.parserFactory.On("MakeParser", "TestRunInputJobSuccess").Return(dp, nil)
	dp.On("Meta").Return(s.meta)

	cancel()
	err := worker.runInputJob(cancelContext, "TestRunInputJobSuccess_2020-03-29.txt")
	assert.Error(s.T(), err)
	s.queryer.AssertNotCalled(s.T(), "CreateTable")
}

func (s *SQLWorkerTestSuite) TestRunInputJobCancelledAtMiddle() {
	cancelContext, cancel := context.WithCancel(context.Background())
	worker := &SQLWorker{
		DB:            s.db,
		Queryer:       s.queryer,
		ParserFactory: s.parserFactory,
		BufferSize:    1,
	}
	dp := new(MockDataParser)
	s.parserFactory.On("MakeParser", "TestRunInputJobCancelledAtMiddle").Return(dp, nil)
	dp.On("Meta").Return(s.meta)
	dp.On("Parse", "TestRunInputJobCancelledAtMiddle_2020-03-29.txt").Return(&parser.DataScanner{
		Metas: s.meta,
		Scanner: bufio.NewScanner(strings.NewReader(`Hello     1  123
World     1  123`)),
	}, nil)
	s.queryer.On("CreateTable", mock.Anything, "TestRunInputJobCancelledAtMiddle", mock.Anything).Return(nil)
	s.queryer.On(
		"InsertData",
		mock.Anything,
		"TestRunInputJobCancelledAtMiddle",
		[]*map[string]interface{}{
			&map[string]interface{}{
				"name":   "Hello",
				"active": true,
				"count":  123,
			},
		},
	).Return(nil)
	s.queryer.On(
		"InsertData",
		mock.Anything,
		"TestRunInputJobCancelledAtMiddle",
		[]*map[string]interface{}{
			&map[string]interface{}{
				"name":   "World",
				"active": true,
				"count":  123,
			},
		},
	).WaitUntil(time.After(1 * time.Second)).Return(nil)

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	err := worker.runInputJob(cancelContext, "TestRunInputJobCancelledAtMiddle_2020-03-29.txt")
	assert.Error(s.T(), err)
}

func TestSQLWorker(t *testing.T) {
	suite.Run(t, new(SQLWorkerTestSuite))
}
