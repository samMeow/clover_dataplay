package main

import (
	"context"
	"data_play/pkg/database"
	"data_play/pkg/parser"
	"data_play/pkg/worker"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
)

func main() {
	var err error
	var wg sync.WaitGroup
	jobChan := make(chan string)
	osChan := make(chan os.Signal)
	closeChan := make(chan int)
	cancelContext, cancel := context.WithCancel(context.Background())

	db := &database.PostgresDB{
		Host:     "localhost",
		Port:     "5433",
		Database: "dataplay",
		Username: "postgres",
		Password: "example",
		Query:    "sslmode=disable",
	}
	err = db.Init()
	if err != nil {
		fmt.Printf("Fail to connecto db %v\n", err)
		os.Exit(1)
	}

	// hardcode current Dir
	currentDir, _ := os.Getwd()
	queryer := &database.QueryerImpl{}
	parserFactory := parser.NewDataParserFactory(currentDir + "/specs/")
	sqlWorker := &worker.SQLWorker{
		DB:            db,
		ParserFactory: parserFactory,
		Queryer:       queryer,
		BufferSize:    50,
	}
	var files []os.FileInfo
	files, err = ioutil.ReadDir(currentDir + "/data")
	if err != nil {
		fmt.Println("fail to read data file")
		os.Exit(1)
	}

	signal.Notify(osChan, os.Interrupt)

	go func() {
		select {
		case <-osChan:
			fmt.Println("OS interrupted exit")
			cancel()
			break
		case <-closeChan:
		}
	}()

	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		go sqlWorker.Start(jobChan, &wg, cancelContext)
	}

jobloop:
	for _, file := range files {
		if filepath.Ext(file.Name()) != ".txt" {
			continue
		}
		select {
		case <-cancelContext.Done():
			break jobloop
		case jobChan <- currentDir + "/data/" + file.Name():
			break
		}
	}

	fmt.Println("Done")
	close(jobChan)
	wg.Wait()
	close(closeChan)
}
