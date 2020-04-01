package execution

import (
	"context"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestAreStreamsEqual(t *testing.T) {
	stateStorage := GetTestStorage(t)
	defer func() {
		go stateStorage.Close()
	}()
	tx := stateStorage.BeginTransaction()
	defer tx.Abort()
	ctx := storage.InjectStateTransaction(context.Background(), tx)

	type args struct {
		first  RecordStream
		second RecordStream
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "empty streams",
			args: args{
				first:  NewInMemoryStream(ctx, []*Record{}),
				second: NewInMemoryStream(ctx, []*Record{}),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "identical streams",
			args: args{
				first: NewInMemoryStream(ctx, []*Record{
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"age"}, []interface{}{7}),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"age"}, []interface{}{10}),
				}),
				second: NewInMemoryStream(ctx, []*Record{
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"age"}, []interface{}{7}),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"age"}, []interface{}{10}),
				}),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "indentical streams with different column order",
			args: args{
				first: NewInMemoryStream(ctx, []*Record{
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"name", "age"}, []interface{}{"Janek", 4}),
				}),
				second: NewInMemoryStream(ctx, []*Record{
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"age", "name"}, []interface{}{4, "Janek"}),
				}),
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "different order streams",
			args: args{
				first: NewInMemoryStream(ctx, []*Record{
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"age"}, []interface{}{10}),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"age"}, []interface{}{7}),
				}),
				second: NewInMemoryStream(ctx, []*Record{
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"age"}, []interface{}{7}),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"age"}, []interface{}{10}),
				}),
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "very complex test",
			args: args{
				first: NewInMemoryStream(ctx, []*Record{
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"age", "name"}, []interface{}{10, "Janek"}),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"name", "age"}, []interface{}{"Wojtek", 7}),
				}),
				second: NewInMemoryStream(ctx, []*Record{
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"age", "name"}, []interface{}{10, "Janek"}),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"name", "age"}, []interface{}{"Wojtek", 7}),
				}),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "not equal streams - mismatched record",
			args: args{
				first: NewInMemoryStream(ctx, []*Record{
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"age", "name"}, []interface{}{10, "Janek"}),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"name", "age"}, []interface{}{"Wojtek", 7}),
				}),
				second: NewInMemoryStream(ctx, []*Record{
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"age", "name"}, []interface{}{7, "Wojtek"}),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"name", "age"}, []interface{}{"Janek", 12}),
				}),
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "mismatched column name",
			args: args{
				first: NewInMemoryStream(ctx, []*Record{
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"age"}, []interface{}{10}),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"age"}, []interface{}{7}),
				}),
				second: NewInMemoryStream(ctx, []*Record{
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"age"}, []interface{}{7}),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"ageButBetter"}, []interface{}{10}),
				}),
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := AreStreamsEqual(ctx, tt.args.first, tt.args.second)
			if (err != nil) != tt.wantErr {
				t.Errorf("AreStreamsEqual() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
