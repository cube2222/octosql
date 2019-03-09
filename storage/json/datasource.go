package json

import (
	"bufio"
	"encoding/json"
	"os"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type JSONDataSource struct {
	path string
}

func (ds *JSONDataSource) Get(variables octosql.Variables) (execution.RecordStream, error) {
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

func (rs *JSONRecordStream) Next() (*execution.Record, error) {
	if rs.isDone {
		return nil, execution.ErrEndOfStream
	}

	if !rs.sc.Scan() {
		rs.isDone = true
		rs.file.Close()
		return nil, execution.ErrEndOfStream
	}

	var record map[octosql.VariableName]interface{}
	err := json.Unmarshal(rs.sc.Bytes(), &record)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't unmarshal json record")
	}

	fields := make([]octosql.VariableName, 0)
	for k := range record {
		fields = append(fields, k)
	}

	return execution.NewRecord(fields, record), nil
}
