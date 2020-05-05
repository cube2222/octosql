package execution

import (
	"context"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

func TestAreStreamsEqual(t *testing.T) {
	stateStorage := storage.GetTestStorage(t)

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
		wantErr bool
	}{
		{
			name: "empty streams",
			args: args{
				first:  NewInMemoryStream(ctx, []*Record{}),
				second: NewInMemoryStream(ctx, []*Record{}),
			},
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

			if err := tt.args.first.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close first in_memory stream: %v", err)
				return
			}
			if err := tt.args.second.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close second in_memory stream: %v", err)
				return
			}
		})
	}
}

func TestAreStreamsEqualNoOrderingWithRetractionReduction(t *testing.T) {
	stateStorage := storage.GetTestStorage(t)
	tx := stateStorage.BeginTransaction()
	ctx := storage.InjectStateTransaction(context.Background(), tx)

	type args struct {
		got  RecordStream
		want RecordStream
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "no retractions, correct ids",
			args: args{
				got: NewInMemoryStream(ctx, []*Record{
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{1, "a"}, WithID(NewRecordID("A.0"))),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{2, "b"}, WithID(NewRecordID("A.2"))),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{3, "c"}, WithID(NewRecordID("A.5"))),
				}),
				want: NewInMemoryStream(ctx, []*Record{
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{2, "b"}),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{3, "c"}),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{1, "a"}),
				}),
			},
			wantErr: false,
		},
		{
			name: "retractions, correct ids",
			args: args{
				got: NewInMemoryStream(ctx, []*Record{
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{1, "a"}, WithID(NewRecordID("A.10"))),             // count 1
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{1, "a"}, WithID(NewRecordID("A.11")), WithUndo()), // count 0
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{1, "a"}, WithID(NewRecordID("A.12")), WithUndo()), // count -1
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{2, "b"}, WithID(NewRecordID("A.20")), WithUndo()), // count -1
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{2, "b"}, WithID(NewRecordID("A.21"))),             // count 0
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{2, "b"}, WithID(NewRecordID("A.22"))),             // count 1
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{3, "c"}, WithID(NewRecordID("A.30"))),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{1, "a"}, WithID(NewRecordID("A.13"))), // count 0
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{3, "c"}, WithID(NewRecordID("A.31"))),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{3, "c"}, WithID(NewRecordID("A.32"))),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{3, "c"}, WithID(NewRecordID("A.33")), WithUndo()), // count is 2

				}),
				want: NewInMemoryStream(ctx, []*Record{
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{2, "b"}),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{3, "c"}),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{3, "c"}),
				}),
			},
			wantErr: false,
		},
		{
			name: "retractions, not correct ids - different bases",
			args: args{
				got: NewInMemoryStream(ctx, []*Record{
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{1, "a"}, WithID(NewRecordID("A.10"))),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{1, "a"}, WithID(NewRecordID("B.12")), WithUndo()),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{2, "b"}, WithID(NewRecordID("A.20"))),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{3, "c"}, WithID(NewRecordID("A.30"))),
				}),
				want: nil,
			},
			wantErr: true,
		},
		{
			name: "retractions, not correct ids - repeating IDS",
			args: args{
				got: NewInMemoryStream(ctx, []*Record{
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{1, "a"}, WithID(NewRecordID("A.1"))),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{1, "a"}, WithID(NewRecordID("A.2")), WithUndo()),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{3, "c"}, WithID(NewRecordID("A.2"))),
					NewRecordFromSliceWithNormalize([]octosql.VariableName{"field1", "field2"}, []interface{}{3, "c"}, WithID(NewRecordID("A.1")), WithUndo()),
				}),
				want: nil,
			},
			wantErr: true,
		},
	}

	err := tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := AreStreamsEqualNoOrderingWithRetractionReductionAndIDChecking(ctx, stateStorage, tt.args.got, tt.args.want, WithEqualityBasedOn(EqualityOfFieldsAndValues))

			if (err != nil) != tt.wantErr {
				t.Errorf("Invalid want error: %v %v", err != nil, tt.wantErr)
			} else if err != nil {
				return
			}
		})
	}
}
