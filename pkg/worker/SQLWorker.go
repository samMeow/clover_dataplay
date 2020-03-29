package worker

import (
	"context"
	"data_play/pkg/database"
	"data_play/pkg/parser"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
)

type SQLWorker struct {
	ParserFactory parser.DataParserFactory
	DB            *database.PostgresDB
	Queryer       database.Queryer
	BufferSize    int
}

func (f *SQLWorker) safeInsertData(cancelContext context.Context, modelName string, data []*map[string]interface{}) error {
	var tx *sqlx.Tx
	var err error

	tx, err = f.DB.Conn().BeginTxx(cancelContext, nil)
	if err != nil {
		return fmt.Errorf("Fail to create Transaction, %v", err)
	}
	err = f.Queryer.InsertData(tx, modelName, data)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("Fail to insert data, %v", err)
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("Fail to commit data, %v", err)
	}
	return nil
}

func (f *SQLWorker) RunInputJob(cancelContext context.Context, dataFile string) error {
	var err error
	var p *parser.DataParser
	var conn *sqlx.DB

	// assume only one _
	modelName := strings.Split(filepath.Base(dataFile), "_")[0]

	p, err = f.ParserFactory.MakeParser(modelName)
	if err != nil {
		return err
	}

	conn = f.DB.Conn()

	select {
	case <-cancelContext.Done():
		return fmt.Errorf("Canceled before create Table %s", modelName)
	default:
		err = f.Queryer.CreateTable(conn, modelName, p.Metas)
	}
	err = f.Queryer.CreateTable(conn, modelName, p.Metas)
	if err != nil {
		return err
	}
	var scanner *parser.DataScanner
	scanner, err = p.Parse(dataFile)
	defer scanner.Close()
	if err != nil {
		return err
	}

	var buffer []*map[string]interface{}
	var datum *map[string]interface{}
	var haveData = true
	var line = 1
loop:
	for {
		select {
		case <-cancelContext.Done():
			err = fmt.Errorf("Canceled")
			break loop
		default:
			datum, haveData, err = scanner.ReadRow()
			if !haveData || err != nil {
				break loop
			}

			buffer = append(buffer, datum)
			if len(buffer) >= f.BufferSize {
				err = f.safeInsertData(cancelContext, modelName, buffer)
				if err != nil {
					err = fmt.Errorf("Inserted Error: line: %d err: %v", line-f.BufferSize, err)
					break loop
				}
				buffer = []*map[string]interface{}{}
			}
			line++

		}
	}
	if err != nil {
		return fmt.Errorf("File %s, inserted: %d line %d failed: %v", dataFile, line-1-len(buffer), line-1, err)
	}
	if len(buffer) > 0 {
		err = f.safeInsertData(cancelContext, modelName, buffer)
	}
	if err != nil {
		return fmt.Errorf("File %s, inserted: %d failed: %v", dataFile, line-1-len(buffer), err)
	}
	fmt.Printf("[Done] File %s inserted: %d\n", dataFile, line-1)
	return nil
}

func (f *SQLWorker) Start(jobChan <-chan string, wg *sync.WaitGroup, cancelContext context.Context) {
	wg.Add(1)
	defer wg.Done()
	for {
		job, ok := <-jobChan
		if !ok {
			return
		}
		err := f.RunInputJob(cancelContext, job)
		if err != nil {
			fmt.Println(err)
		}
	}
}
