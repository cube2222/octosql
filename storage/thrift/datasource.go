package thrift

import (
	"context"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/pkg/errors"
	"sort"
)

var availableFilters = map[physical.FieldType]map[physical.Relation]struct{}{
	physical.Primary:   make(map[physical.Relation]struct{}),
	physical.Secondary: make(map[physical.Relation]struct{}),
}

type DataSource struct {
	alias       string
	thriftAddr  string
	protocol    string
	secure      bool
}

func NewDataSourceBuilderFactory() physical.DataSourceBuilderFactory {
	return physical.NewDataSourceBuilderFactory(
		func(ctx context.Context, matCtx *physical.MaterializationContext, dbConfig map[string]interface{}, filter physical.Formula, alias string) (execution.Node, error) {

			thriftAddr, err := config.GetString(dbConfig, "ip", config.WithDefault("localhost:9090"))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get thrift server address")
			}

			protocol, err := config.GetString(dbConfig, "protocol", config.WithDefault("binary"))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get thrift server protocol")
			}

			secure, err := config.GetBool(dbConfig, "secure", config.WithDefault(false))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get thrift server secure option")
			}

			return &DataSource{
				alias:       alias,
				thriftAddr:  thriftAddr,
				protocol:    protocol,
				secure:      secure,
			}, nil
		},
		nil,
		availableFilters,
		metadata.BoundedFitsInLocalStorage,
	)
}

// NewDataSourceBuilderFactoryFromConfig creates a data source builder factory using the configuration.
func NewDataSourceBuilderFactoryFromConfig(dbConfig map[string]interface{}) (physical.DataSourceBuilderFactory, error) {
	return NewDataSourceBuilderFactory(), nil
}

func (ds *DataSource) Get(variables octosql.Variables) (execution.RecordStream, error) {

	return &RecordStream{
		isDone:                        false,
		alias:                         ds.alias,
		source:                        *ds,
		streamID:                      0,
		isOpen:                        false,
	}, nil
}

type RecordStream struct {
	isDone                        bool
	alias                         string
	source                        DataSource
	streamID                      int32
	isOpen                        bool
}

func (rs *RecordStream) Close() error {
	return nil
}

func (rs *RecordStream) Next() (*execution.Record, error) {
	if rs.isDone {
		return nil, execution.ErrEndOfStream
	}

	result, err := RunClient(rs)
	if err != nil {
		return nil, err
	}

	if len(result.Fields) == 0 {
		rs.isDone = true
		return nil, execution.ErrEndOfStream
	}

	aliasedRecord := result.Fields

	fields := make([]octosql.VariableName, 0)
	for k := range aliasedRecord {
		fields = append(fields, k)
	}

	sort.Slice(fields, func(i, j int) bool {
		return fields[i] < fields[j]
	})
	return execution.NewRecord(fields, aliasedRecord), nil
}
