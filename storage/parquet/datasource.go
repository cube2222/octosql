package parquet

import (
	"context"
	"io"
	"log"
	"reflect"

	"github.com/pkg/errors"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"

	"github.com/fatih/structs"

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
	path  string
	alias string
}

func NewDataSourceBuilderFactory() physical.DataSourceBuilderFactory {
	return physical.NewDataSourceBuilderFactory(
		func(ctx context.Context, matCtx *physical.MaterializationContext, dbConfig map[string]interface{}, filter physical.Formula, alias string) (execution.Node, error) {
			path, err := config.GetString(dbConfig, "path")
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get path")
			}

			return &DataSource{
				path:  path,
				alias: alias,
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

func (ds *DataSource) Get(ctx context.Context, variables octosql.Variables) (execution.RecordStream, error) {
	file, err := local.NewLocalFileReader(ds.path)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't open file")
	}
	reader, err := reader.NewParquetReader(file, nil, 4)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create parquet reader")
	}
	return &RecordStream{
		file:          file,
		isDone:        false,
		parquetReader: reader,
		alias:         ds.alias,
	}, nil
}

type RecordStream struct {
	file          source.ParquetFile
	parquetReader *reader.ParquetReader
	isDone        bool
	alias         string
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

	res, err := rs.parquetReader.ReadByNumber(1)
	if err == io.EOF {
		rs.isDone = true
		rs.file.Close()
		return nil, execution.ErrEndOfStream
	}
	m := structs.Map(res[0])
	for k, v := range m {
		log.Printf("%s: %v of type %+v", k, v, reflect.TypeOf(v).String())
	}
	log.Printf("%+v", m)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't read record")
	}

	aliasedRecord := make(map[octosql.VariableName]octosql.Value)
	for i := range res {
		_ = i // res is unreadable pls help
		// aliasedRecord[rs.aliasedFields[i]] = execution.ParseType(v)
	}

	return execution.NewRecord(nil, aliasedRecord), nil
}
