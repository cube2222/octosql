package json

import (
	"bufio"
	"encoding/json"
	"os"

	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type JSONDataSource struct {
	path string
}

func (ds *JSONDataSource) Get(primitiveValues map[string]interface{}) (octosql.RecordStream, error) {
	file, err := os.Open(ds.path)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't open file")
	}
	sc := bufio.NewScanner(file)
	return &JSONRecordStream{
		file:   file,
		sc:     sc,
		isDone: false,
	}, nil
}

type JSONRecordStream struct {
	file   *os.File
	sc     *bufio.Scanner
	isDone bool
}

func (rs *JSONRecordStream) Next() (*octosql.Record, error) {
	if rs.isDone {
		return nil, octosql.ErrEndOfStream
	}

	if !rs.sc.Scan() {
		rs.isDone = true
		rs.file.Close()
		return nil, octosql.ErrEndOfStream
	}

	var record map[string]interface{}
	err := json.Unmarshal(rs.sc.Bytes(), &record)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't unmarshal json record")
	}

	return octosql.NewRecord(record), nil
}
