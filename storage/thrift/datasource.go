package thrift

import (
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/storage/thrift/source"
)

// Create new Thrift data source factory
func NewDataSourceBuilderFactory() physical.DataSourceBuilderFactory {
	return source.NewThriftDataSourceBuilderFactory()
}

// NewDataSourceBuilderFactoryFromConfig creates a data source builder factory using the configuration.
func NewDataSourceBuilderFactoryFromConfig(dbConfig map[string]interface{}) (physical.DataSourceBuilderFactory, error) {
	return source.NewThriftDataSourceBuilderFactory(), nil
}