package execution

import (
	"context"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

func TestFilteredStream_Next(t *testing.T) {
	stateStorage := storage.GetTestStorage(t)

	tx := stateStorage.BeginTransaction()
	defer tx.Abort()
	ctx := storage.InjectStateTransaction(context.Background(), tx)

	fieldNames := []octosql.VariableName{
		octosql.NewVariableName("age"),
		octosql.NewVariableName("something"),
	}

	type fields struct {
		formula   Formula
		variables octosql.Variables
		source    RecordStream
	}
	tests := []struct {
		name    string
		fields  fields
		want    RecordStream
		wantErr bool
	}{
		{
			name: "simple filter",
			fields: fields{
				formula: NewPredicate(
					NewVariable("age"),
					NewNotEqual(),
					NewVariable("const"),
				),
				variables: map[octosql.VariableName]octosql.Value{
					octosql.NewVariableName("const"): octosql.MakeInt(3),
				},
				source: NewInMemoryStream(ctx, []*Record{
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{5, "test"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{4, "test2"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{3, "test3"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{3, "test33"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{2, "test2"},
					),
				}),
			},
			want: NewInMemoryStream(ctx, []*Record{
				NewRecordFromSliceWithNormalize(
					fieldNames,
					[]interface{}{5, "test"},
				),
				NewRecordFromSliceWithNormalize(
					fieldNames,
					[]interface{}{4, "test2"},
				),
				NewRecordFromSliceWithNormalize(
					fieldNames,
					[]interface{}{2, "test2"},
				),
			}),
			wantErr: false,
		},
		{
			name: "filter with duplicates",
			fields: fields{
				formula: NewPredicate(
					NewVariable("age"),
					NewNotEqual(),
					NewVariable("const"),
				),
				variables: map[octosql.VariableName]octosql.Value{
					octosql.NewVariableName("const"): octosql.MakeInt(3),
				},
				source: NewInMemoryStream(ctx, []*Record{
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{5, "test"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{5, "test"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{4, "test2"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{3, "test3"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{3, "test33"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{2, "test2"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{2, "test2"},
					),
				}),
			},
			want: NewInMemoryStream(ctx, []*Record{
				NewRecordFromSliceWithNormalize(
					fieldNames,
					[]interface{}{5, "test"},
				),
				NewRecordFromSliceWithNormalize(
					fieldNames,
					[]interface{}{5, "test"},
				),
				NewRecordFromSliceWithNormalize(
					fieldNames,
					[]interface{}{4, "test2"},
				),
				NewRecordFromSliceWithNormalize(
					fieldNames,
					[]interface{}{2, "test2"},
				),
				NewRecordFromSliceWithNormalize(
					fieldNames,
					[]interface{}{2, "test2"},
				),
			}),
			wantErr: false,
		},
		{
			name: "empty stream",
			fields: fields{
				formula: NewPredicate(
					NewVariable("age"),
					NewNotEqual(),
					NewVariable("const"),
				),
				variables: map[octosql.VariableName]octosql.Value{
					octosql.NewVariableName("const"): octosql.MakeInt(3),
				},
				source: NewInMemoryStream(ctx, nil),
			},
			want:    NewInMemoryStream(ctx, nil),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := &FilteredStream{
				formula:   tt.fields.formula,
				variables: tt.fields.variables,
				source:    tt.fields.source,
			}
			err := AreStreamsEqual(ctx, stream, tt.want)
			if (err != nil) != tt.wantErr {
				t.Errorf("FilteredStream.Next() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err := stream.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close filter stream: %v", err)
				return
			}
			if err := tt.want.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close wanted in_memory stream: %v", err)
				return
			}
		})
	}
}
