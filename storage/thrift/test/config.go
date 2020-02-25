package test

import (
	"context"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/storage/thrift/source"
)

// Creates test data source for a given test case configuration
func CreateTestSource(ctx context.Context, testCase ThriftTestCase) (error, *execution.Node) {
	err, inputStream, outputStream := CreateThriftFakeIO(ctx, testCase.data)
	if err != nil {
		panic(err)
	}

	ds, err := source.NewThriftDataSourceBuilderFactory()("test", testCase.alias).Materialize(ctx, &physical.MaterializationContext{
		Config: &config.Config{
			DataSources: []config.DataSourceConfig{
				{
					Name: "test",
					Config: map[string]interface{}{
						"ip": testCase.ip,
						"protocol": testCase.protocol,
						"secure": testCase.secure,
						"inputStream": inputStream,
						"outputStream": outputStream,
						"thriftSpecsString": testCase.thriftMeta,
					},
				},
			},
		},
	})

	if err != nil {
		return err, nil
	}

	return nil, &ds
}