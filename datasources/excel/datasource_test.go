package excel

import (
	"context"
	"testing"
	"time"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/storage"
)

func Test_getCellRowCol(t *testing.T) {
	type args struct {
		cell string
	}
	tests := []struct {
		name    string
		args    args
		wantRow string
		wantCol string
	}{
		{
			name: "single column, single row",
			args: args{
				cell: "A1",
			},
			wantCol: "A",
			wantRow: "1",
		},

		{
			name: "multi column, single row",
			args: args{
				cell: "ABZZ1",
			},
			wantCol: "ABZZ",
			wantRow: "1",
		},

		{
			name: "single column, multi row",
			args: args{
				cell: "A1234",
			},
			wantCol: "A",
			wantRow: "1234",
		},

		{
			name: "multi column, multi row",
			args: args{
				cell: "ABZ1234",
			},
			wantCol: "ABZ",
			wantRow: "1234",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRow, gotCol := getRowAndColumnFromCell(tt.args.cell)
			if gotRow != tt.wantRow {
				t.Errorf("getRowAndColumnFromCell() gotRow = %v, want %v", gotRow, tt.wantRow)
			}
			if gotCol != tt.wantCol {
				t.Errorf("getRowAndColumnFromCell() gotCol = %v, want %v", gotCol, tt.wantCol)
			}
		})
	}
}

func Test_getRowColCoords(t *testing.T) {
	type args struct {
		cell string
	}
	tests := []struct {
		name    string
		args    args
		wantRow int
		wantCol int
		wantErr bool
	}{
		{
			name: "single column single row",
			args: args{
				cell: "C4",
			},
			wantRow: 3,
			wantCol: 2,
			wantErr: false,
		},

		{
			name: "multi column single row",
			args: args{
				cell: "AA9",
			},
			wantRow: 8,
			wantCol: 26,
			wantErr: false,
		},

		{
			name: "single column multi row",
			args: args{
				cell: "A123",
			},
			wantRow: 122,
			wantCol: 0,
			wantErr: false,
		},

		{
			name: "multi column multi row",
			args: args{
				cell: "AD982",
			},
			wantRow: 981,
			wantCol: 29,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRow, gotCol, err := getCoordinatesFromCell(tt.args.cell)
			if (err != nil) != tt.wantErr {
				t.Errorf("getCoordinatesFromCell() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotRow != tt.wantRow {
				t.Errorf("getCoordinatesFromCell() gotRow = %v, want %v", gotRow, tt.wantRow)
			}
			if gotCol != tt.wantCol {
				t.Errorf("getCoordinatesFromCell() gotCol = %v, want %v", gotCol, tt.wantCol)
			}
		})
	}
}

func TestDataSource_Get(t *testing.T) {
	ctx := context.Background()
	streamId := execution.GetRawStreamID()

	type fields struct {
		path         string
		alias        string
		hasHeaderRow bool
		sheet        string
		rootCell     string
		timeColumns  []interface{}
	}
	type args struct {
		variables octosql.Variables
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []*execution.Record
	}{
		{
			name: "simple default config test",
			fields: fields{
				path:         "fixtures/test.xlsx",
				alias:        "t",
				hasHeaderRow: true,
				sheet:        "Sheet1",
				rootCell:     "A1",
				timeColumns:  []interface{}{},
			},
			args: args{
				variables: octosql.NoVariables(),
			},
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.name", "t.surname", "t.age"},
					[]interface{}{"Jan", "Chomiak", 20},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.name", "t.surname", "t.age"},
					[]interface{}{"Kuba", "Martin", 21},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.name", "t.surname", "t.age"},
					[]interface{}{"Wojtek", "Kuźmiński", 21},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2)),
				),
			},
		},

		{
			name: "simple modified config test",
			fields: fields{
				path:         "fixtures/test.xlsx",
				alias:        "t",
				hasHeaderRow: false,
				sheet:        "CustomSheet",
				rootCell:     "B3",
				timeColumns:  []interface{}{},
			},
			args: args{
				variables: octosql.NoVariables(),
			},
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.col1", "t.col2"},
					[]interface{}{"Warsaw", 1700000},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.col1", "t.col2"},
					[]interface{}{"Atlanta", 2000},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.col1", "t.col2"},
					[]interface{}{"New York", 2},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.col1", "t.col2"},
					[]interface{}{"Miami", -5},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 3)),
				),
			},
		},

		{
			name: "table with preceeding data",
			fields: fields{
				path:         "fixtures/test.xlsx",
				alias:        "t",
				hasHeaderRow: true,
				sheet:        "CustomSheet",
				rootCell:     "E2",
				timeColumns:  []interface{}{},
			},
			args: args{
				variables: octosql.NoVariables(),
			},
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.id", "t.points"},
					[]interface{}{1, 10},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.id", "t.points"},
					[]interface{}{2, 4},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.id", "t.points"},
					[]interface{}{3, 19},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2)),
				),
			},
		},

		{
			name: "table with nil inside",
			fields: fields{
				path:         "fixtures/test.xlsx",
				alias:        "t",
				hasHeaderRow: true,
				sheet:        "CustomSheet",
				rootCell:     "A9",
				timeColumns:  []interface{}{},
			},
			args: args{
				variables: octosql.NoVariables(),
			},
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.name", "t.age", "t.id"},
					[]interface{}{"Bob", 13, 1},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.name", "t.age", "t.id"},
					[]interface{}{"Ally", nil, 2},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.name", "t.age", "t.id"},
					[]interface{}{nil, 7, nil},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2)),
				),
			},
		},

		{
			name: "dates with no header row",
			fields: fields{
				path:         "fixtures/test.xlsx",
				alias:        "t",
				hasHeaderRow: false,
				sheet:        "DateSheet",
				rootCell:     "A2",
				timeColumns:  []interface{}{"col1"},
			},
			args: args{
				variables: octosql.NoVariables(),
			},
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.col1", "t.col2"},
					[]interface{}{time.Date(2017, 3, 14, 13, 0, 0, 0, time.UTC), 1},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.col1", "t.col2"},
					[]interface{}{time.Date(2017, 3, 15, 13, 0, 0, 0, time.UTC), 2},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.col1", "t.col2"},
					[]interface{}{time.Date(2019, 5, 19, 14, 0, 0, 0, time.UTC), 3},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2)),
				),
			},
		},

		{
			name: "dates with header row",
			fields: fields{
				path:         "fixtures/test.xlsx",
				alias:        "t",
				hasHeaderRow: true,
				sheet:        "DateSheet",
				rootCell:     "D3",
				timeColumns:  []interface{}{"date"},
			},
			args: args{
				variables: octosql.NoVariables(),
			},
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.date", "t.points"},
					[]interface{}{time.Date(2017, 3, 14, 13, 0, 0, 0, time.UTC), 101},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 0)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.date", "t.points"},
					[]interface{}{time.Date(2017, 3, 15, 13, 0, 0, 0, time.UTC), 102},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 1)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"t.date", "t.points"},
					[]interface{}{time.Date(2019, 5, 19, 14, 0, 0, 0, time.UTC), 103},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamId, 2)),
				),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stateStorage := storage.GetTestStorage(t)

			ds, err := NewDataSourceBuilderFactory()("test", tt.fields.alias)[0].Materialize(context.Background(), &physical.MaterializationContext{
				Config: &config.Config{
					DataSources: []config.DataSourceConfig{
						{
							Name: "test",
							Config: map[string]interface{}{
								"path":        tt.fields.path,
								"headerRow":   tt.fields.hasHeaderRow,
								"sheet":       tt.fields.sheet,
								"rootCell":    tt.fields.rootCell,
								"timeColumns": tt.fields.timeColumns,
								"batchSize":   2,
							},
						},
					},
				},
				Storage: stateStorage,
			})
			if err != nil {
				t.Errorf("Error creating data source: %v", err)
			}

			got := execution.GetTestStream(t, stateStorage, octosql.NoVariables(), ds, execution.GetTestStreamWithStreamID(streamId))

			tx := stateStorage.BeginTransaction()
			want, _, err := execution.NewDummyNode(tt.want).Get(storage.InjectStateTransaction(ctx, tx), octosql.NoVariables(), streamId)
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			if err := execution.AreStreamsEqualNoOrdering(storage.InjectStateTransaction(ctx, tx), stateStorage, want, got); err != nil {
				t.Errorf("Streams aren't equal: %v", err)
				return
			}

			if err := got.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close excel stream: %v", err)
				return
			}
			if err := want.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close wanted in_memory stream: %v", err)
				return
			}
		})
	}
}
