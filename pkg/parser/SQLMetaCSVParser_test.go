package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSuccessful(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	parser := &SQLMetaCSVParser{
		filePath: "TestParseSuccessful",
		buffer: []byte(`"column_name","size","datatype"
name,10,TEXT
count,5,INTEGER
active,1,BOOLEAN`),
	}
	meta, err := parser.Parse()
	assert.Nil(err)
	assert.Equal(meta, []*SQLMeta{
		&SQLMeta{
			Name:     "name",
			Size:     10,
			DataType: "TEXT",
		},
		&SQLMeta{
			Name:     "count",
			Size:     5,
			DataType: "INTEGER",
		},
		&SQLMeta{
			Name:     "active",
			Size:     1,
			DataType: "BOOLEAN",
		},
	})
}

func TestParseUTF8(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	parser := &SQLMetaCSVParser{
		filePath: "TestParseSuccessful",
		buffer: []byte(`"column_name","size","datatype"
名前,10,TEXT`),
	}
	meta, err := parser.Parse()
	assert.Nil(err)
	assert.Equal(meta, []*SQLMeta{
		&SQLMeta{
			Name:     "名前",
			Size:     10,
			DataType: "TEXT",
		},
	})
}

func TestParseFail(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	parser := &SQLMetaCSVParser{
		filePath: "TestParseSuccessful",
		buffer: []byte(`"column_name","size","datatype"
名前,10,TEXT,`),
	}
	_, err := parser.Parse()
	assert.Error(err)
}
