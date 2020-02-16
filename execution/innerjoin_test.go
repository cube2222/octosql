package execution

import (
	"context"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestInnerJoinedStream_Next(t *testing.T) {
	fieldNames := []string{
		"bike",
		"name",
	}
	fieldNames2 := []string{
		"color",
		"score",
	}

	type fields struct {
		variables octosql.Variables
		source    RecordStream
		joined    Node
	}
	tests := []struct {
		name    string
		fields  fields
		want    RecordStream
		wantErr bool
	}{
		{
			name: "simple inner join",
			fields: fields{
				variables: map[octosql.VariableName]octosql.Value{
					octosql.NewVariableName("const"): octosql.MakeInt(3),
				},
				source: NewInMemoryStream(
					[]*Record{
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{"red", "test"},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{"blue", "test2"},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{"green", "test3"},
						),
					},
				),
				joined: NewFilter(
					NewAnd(
						NewPredicate(
							NewVariable(octosql.NewVariableName("score")),
							&MoreThan{},
							NewVariable(octosql.NewVariableName("const")),
						),
						NewPredicate(
							NewVariable(octosql.NewVariableName("bike")),
							&Equal{},
							NewVariable(octosql.NewVariableName("color")),
						),
					),
					NewDummyNode([]*Record{
						NewRecordFromSliceWithNormalize(
							fieldNames2,
							[]interface{}{"green", 7},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames2,
							[]interface{}{"red", 5},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames2,
							[]interface{}{"green", 4},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames2,
							[]interface{}{"green", 2},
						),
					})),
			},
			want: NewInMemoryStream(
				[]*Record{
					NewRecordFromSliceWithNormalize(
						[]string{"bike", "name", "color", "score"},
						[]interface{}{"red", "test", "red", 5},
					),
					NewRecordFromSliceWithNormalize(
						[]string{"bike", "name", "color", "score"},
						[]interface{}{"green", "test3", "green", 7},
					),
					NewRecordFromSliceWithNormalize(
						[]string{"bike", "name", "color", "score"},
						[]interface{}{"green", "test3", "green", 4},
					),
				},
			),
			wantErr: false,
		},
		{
			name: "empty stream",
			fields: fields{
				variables: map[octosql.VariableName]octosql.Value{
					octosql.NewVariableName("const"): octosql.MakeInt(3),
				},
				source: NewInMemoryStream(nil),
				joined: NewDummyNode(nil),
			},
			want:    NewInMemoryStream(nil),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := &InnerJoinedStream{
				joiner: NewJoiner(
					2,
					tt.fields.variables,
					tt.fields.source,
					tt.fields.joined,
				),
			}
			stateStorage := GetTestStorage(t)
			tx := stateStorage.BeginTransaction()
			ctx := storage.InjectStateTransaction(context.Background(), tx)

			equal, err := AreStreamsEqual(ctx, stream, tt.want)
			if (err != nil) != tt.wantErr {
				t.Errorf("InnerJoinedStream.Next() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && !equal {
				t.Errorf("InnerJoinedStream.Next() streams not equal")
			}

			tx.Commit()
		})
	}
}
