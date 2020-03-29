package parser

import "sync"

type DataParserFactory interface {
	MakeParser(modelName string) (DataParser, error)
}

var singleton *DataParserFactoryImpl = nil

type DataParserFactoryImpl struct {
	SpecDir string
	Cache   *sync.Map
}

func NewDataParserFactory(specDir string) DataParserFactory {
	if singleton != nil {
		return singleton
	}
	singleton = &DataParserFactoryImpl{
		SpecDir: specDir,
		Cache:   &sync.Map{},
	}
	return singleton
}

func (dpf *DataParserFactoryImpl) MakeParser(modelName string) (DataParser, error) {
	var err error
	if parser, ok := dpf.Cache.Load(modelName); ok {
		return parser.(DataParser), nil
	}
	specFile := dpf.SpecDir + modelName + ".csv"
	var meta []*SQLMeta
	var sqlparser *SQLMetaCSVParser
	sqlparser, err = NewSQLMetaCSVParser(specFile)
	if err != nil {
		return nil, err
	}
	meta, err = sqlparser.Parse()
	if err != nil {
		return nil, err
	}
	p := NewDataParser(meta)
	dpf.Cache.Store(modelName, p)
	return p, nil
}
