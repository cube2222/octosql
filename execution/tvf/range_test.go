package tvf

import (
	"context"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestRange_Get(t *testing.T) {
	ctx := context.Background()
	type fields struct {
		start execution.Expression
		end   execution.Expression
	}
	type args struct {
		variables octosql.Variables
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    execution.RecordStream
		wantErr bool
	}{
		{
			name: "simple range",
			fields: fields{
				start: execution.NewVariable(octosql.NewVariableName("start")),
				end:   execution.NewVariable(octosql.NewVariableName("end")),
			},
			args: args{
				variables: octosql.NewVariables(map[octosql.VariableName]octosql.Value{
					"start": octosql.MakeInt(1),
					"end":   octosql.MakeInt(10),
				}),
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"i"},
					[]interface{}{1},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"i"},
					[]interface{}{2},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"i"},
					[]interface{}{3},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"i"},
					[]interface{}{4},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"i"},
					[]interface{}{5},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"i"},
					[]interface{}{6},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"i"},
					[]interface{}{7},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"i"},
					[]interface{}{8},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"i"},
					[]interface{}{9},
				),
			}),
			wantErr: false,
		},
		{
			name: "simple range",
			fields: fields{
				start: execution.NewVariable(octosql.NewVariableName("start")),
				end:   execution.NewVariable(octosql.NewVariableName("end")),
			},
			args: args{
				variables: octosql.NewVariables(map[octosql.VariableName]octosql.Value{
					"start": octosql.MakeInt(5),
					"end":   octosql.MakeInt(10),
				}),
			},
			want: execution.NewInMemoryStream([]*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"i"},
					[]interface{}{5},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"i"},
					[]interface{}{6},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"i"},
					[]interface{}{7},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"i"},
					[]interface{}{8},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"i"},
					[]interface{}{9},
				),
			}),
			wantErr: false,
		},
		{
			name: "empty range",
			fields: fields{
				start: execution.NewVariable(octosql.NewVariableName("start")),
				end:   execution.NewVariable(octosql.NewVariableName("end")),
			},
			args: args{
				variables: octosql.NewVariables(map[octosql.VariableName]octosql.Value{
					"start": octosql.MakeInt(3),
					"end":   octosql.MakeInt(3),
				}),
			},
			want:    execution.NewInMemoryStream([]*execution.Record{}),
			wantErr: false,
		},
		{
			name: "reverse range",
			fields: fields{
				start: execution.NewVariable(octosql.NewVariableName("start")),
				end:   execution.NewVariable(octosql.NewVariableName("end")),
			},
			args: args{
				variables: octosql.NewVariables(map[octosql.VariableName]octosql.Value{
					"start": octosql.MakeInt(4),
					"end":   octosql.MakeInt(3),
				}),
			},
			want:    execution.NewInMemoryStream([]*execution.Record{}),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Range{
				start: tt.fields.start,
				end:   tt.fields.end,
			}

			stateStorage := execution.GetTestStorage(t)
			tx := stateStorage.BeginTransaction()
			ctx := storage.InjectStateTransaction(ctx, tx)

			got, _, err := r.Get(ctx, tt.args.variables, execution.GetRawStreamID())
			if (err != nil) != tt.wantErr {
				t.Errorf("Range.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			eq, err := execution.AreStreamsEqual(ctx, got, tt.want)
			if err != nil {
				t.Errorf("Range.Get() AreStreamsEqual error = %v", err)
			}
			if !eq {
				t.Errorf("Range.Get() streams not equal")
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
