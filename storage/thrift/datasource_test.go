package thrift

import (
	"context"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/storage/thrift/test"
	"testing"
)

func TestThriftRecordStream_Get(t *testing.T) {

	for _, tt := range test.THRIFT_DATA_SOURCE_TEST_CASES {
		t.Run(tt.TestCaseName(), func(t *testing.T) {
			ctx := context.Background()
			err, ds := test.CreateTestSource(ctx, tt)
			if err != nil {
				t.Errorf("Error creating data source: %v", err)
			}

			got, err := (*ds).Get(ctx, octosql.NoVariables())
			if err != nil {
				t.Errorf("DataSource.Get() error: %v", err)
				return
			}

			if ok, err := execution.AreStreamsEqual(ctx, tt.WantedOutput(), got); !ok {
				t.Errorf("Streams aren't equal: %v", err)
				return
			}
		})
	}

}
