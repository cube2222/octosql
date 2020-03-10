package json

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/metadata"
)

var availableFilters = map[physical.FieldType]map[physical.Relation]struct{}{
	physical.Primary:   make(map[physical.Relation]struct{}),
	physical.Secondary: make(map[physical.Relation]struct{}),
}

type DataSource struct {
	path        string
	alias       string
	arrayFormat bool
}

func NewDataSourceBuilderFactory() physical.DataSourceBuilderFactory {
	return physical.NewDataSourceBuilderFactory(
		func(ctx context.Context, matCtx *physical.MaterializationContext, dbConfig map[string]interface{}, filter physical.Formula, alias string, partition int) (execution.Node, error) {
			path, err := config.GetString(dbConfig, "path")
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get path")
			}
			arrayFormat, err := config.GetBool(dbConfig, "arrayFormat", config.WithDefault(false))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get if json in array form")
			}

			return &DataSource{
				path:        path,
				arrayFormat: arrayFormat,
				alias:       alias,
			}, nil
		},
		nil,
		availableFilters,
		metadata.BoundedFitsInLocalStorage,
		1,
	)
}

// NewDataSourceBuilderFactoryFromConfig creates a data source builder factory using the configuration.
func NewDataSourceBuilderFactoryFromConfig(dbConfig map[string]interface{}) (physical.DataSourceBuilderFactory, error) {
	return NewDataSourceBuilderFactory(), nil
}

func (ds *DataSource) Get(ctx context.Context, variables octosql.Variables, streamID *execution.StreamID) (execution.RecordStream, *execution.ExecutionOutput, error) {
	file, err := os.Open(ds.path)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't open file")
	}

	return &RecordStream{
		arrayFormat:                   ds.arrayFormat,
		arrayFormatOpeningBracketRead: false,
		file:                          file,
		decoder:                       json.NewDecoder(file),
		isDone:                        false,
		alias:                         ds.alias,
	}, execution.NewExecutionOutput(execution.NewZeroWatermarkGenerator()), nil
}

type RecordStream struct {
	arrayFormat                   bool
	arrayFormatOpeningBracketRead bool
	file                          *os.File
	decoder                       *json.Decoder
	isDone                        bool
	alias                         string
}

func (rs *RecordStream) Close() error {
	err := rs.file.Close()
	if err != nil {
		return errors.Wrap(err, "Couldn't close underlying file")
	}

	return nil
}

func (rs *RecordStream) Next(ctx context.Context) (*execution.Record, error) {
	if rs.isDone {
		return nil, execution.ErrEndOfStream
	}

	if rs.arrayFormat && !rs.arrayFormatOpeningBracketRead {
		tok, err := rs.decoder.Token() // Read opening [
		if tok != json.Delim('[') {
			return nil, errors.Errorf("expected [ as first json token, got %v", tok)
		}
		if err != nil {
			return nil, errors.Wrap(err, "couldn't read json opening bracket")
		}
		rs.arrayFormatOpeningBracketRead = true
	}

	if !rs.decoder.More() {
		rs.isDone = true
		rs.file.Close()
		return nil, execution.ErrEndOfStream
	}

	var record map[octosql.VariableName]interface{}
	err := rs.decoder.Decode(&record)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't decode json record")
	}

	aliasedRecord := make(map[octosql.VariableName]octosql.Value)
	for k, v := range record {
		if str, ok := v.(string); ok {
			parsed, err := time.Parse(time.RFC3339, str)
			if err == nil {
				v = parsed
			}
		}
		aliasedRecord[octosql.NewVariableName(fmt.Sprintf("%s.%s", rs.alias, k))] = octosql.NormalizeType(v)
	}

	fields := make([]octosql.VariableName, 0)
	for k := range aliasedRecord {
		fields = append(fields, k)
	}

	sort.Slice(fields, func(i, j int) bool {
		return fields[i] < fields[j]
	})

	return execution.NewRecord(fields, aliasedRecord), nil
}
