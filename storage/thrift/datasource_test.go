package thrift

import (
	"context"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/storage/thrift/demo/client"
	"github.com/cube2222/octosql/storage/thrift/demo/server"
	"sync"
	"testing"
)

func SendServerStop(addr string, protocol string, secure bool) {
	transportFactory := thrift.NewTTransportFactory()
	protocolFactory := GetProtocolFactory(protocol)

	err := client.RunClientStopServer(transportFactory, protocolFactory, addr, secure)
	if err != nil {
		fmt.Print(err)
	}
}

func StartServer(addr string, protocol string, secure bool, wg *sync.WaitGroup) {
	transportFactory := thrift.NewTTransportFactory()
	protocolFactory := GetProtocolFactory(protocol)

	err := server.RunServer(transportFactory, protocolFactory, addr, secure)
	if err != nil {
		fmt.Print(err)
	}
	wg.Done()
}

func TestThriftRecordStream_Get(t *testing.T) {

	wantData := execution.NewInMemoryStream(
		[]*execution.Record{
			execution.NewRecordFromSliceWithNormalize(
				[]octosql.VariableName{"b.field_0.field_0", "b.field_0.field_1"},
				[]interface{}{"record0", "B"},
			),
			execution.NewRecordFromSliceWithNormalize(
				[]octosql.VariableName{"b.field_0.field_0", "b.field_0.field_1"},
				[]interface{}{"record1", "B"},
			),
			execution.NewRecordFromSliceWithNormalize(
				[]octosql.VariableName{"b.field_0.field_0", "b.field_0.field_1"},
				[]interface{}{"record2", "B"},
			),
			execution.NewRecordFromSliceWithNormalize(
				[]octosql.VariableName{"b.field_0.field_0", "b.field_0.field_1"},
				[]interface{}{"record3", "B"},
			),
			execution.NewRecordFromSliceWithNormalize(
				[]octosql.VariableName{"b.field_0.field_0", "b.field_0.field_1"},
				[]interface{}{"record4", "B"},
			),
		},
	)

	tests := []struct {
		name        string
		ip          string
		protocol    string
		secure      bool
		alias       string
		want        execution.RecordStream
	}{
		{
			name:     "reading thrift server",
			ip:       "localhost:9090",
			protocol: "binary",
			secure:   false,
			alias:    "b",
			want:     wantData,
		},
	}

	for _, tt := range tests {

		var wg sync.WaitGroup = sync.WaitGroup{}
		wg.Add(1)
		go StartServer(tt.ip, tt.protocol, tt.secure, &wg)

		t.Run(tt.name, func(t *testing.T) {
			ds, err := NewDataSourceBuilderFactory()("test", tt.alias).Materialize(context.Background(), &physical.MaterializationContext{
				Config: &config.Config{
					DataSources: []config.DataSourceConfig{
						{
							Name: "test",
							Config: map[string]interface{}{
								"ip": tt.ip,
								"protocol": tt.protocol,
								"secure": tt.secure,
							},
						},
					},
				},
			})
			if err != nil {
				t.Errorf("Error creating data source: %v", err)
			}

			got, err := ds.Get(octosql.NoVariables())
			if err != nil {
				t.Errorf("DataSource.Get() error: %v", err)
				return
			}

			if ok, err := execution.AreStreamsEqual(tt.want, got); !ok {
				t.Errorf("Streams aren't equal: %v", err)
				return
			}
		})

		SendServerStop(tt.ip, tt.protocol, tt.secure)
		wg.Wait()
	}

}
