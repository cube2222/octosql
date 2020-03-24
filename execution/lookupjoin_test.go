package execution

import (
	"context"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestLookupJoin(t *testing.T) {
	fieldNames := []octosql.VariableName{
		octosql.NewVariableName("bike"),
		octosql.NewVariableName("name"),
	}
	fieldNames2 := []octosql.VariableName{
		octosql.NewVariableName("color"),
		octosql.NewVariableName("score"),
	}

	type fields struct {
		variables   octosql.Variables
		source      Node
		joined      Node
		maxJobCount int
		isLeftJoin  bool
	}
	tests := []struct {
		name    string
		fields  fields
		want    Node
		wantErr bool
	}{
		{
			name: "simple left join",
			fields: fields{
				maxJobCount: 1,
				isLeftJoin:  true,
				variables: map[octosql.VariableName]octosql.Value{
					octosql.NewVariableName("const"): octosql.MakeInt(3),
				},
				source: NewDummyNode(
					[]*Record{
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{"red", "test"},
							WithID(NewRecordID("00")),
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{"blue", "test2"},
							WithID(NewRecordID("01")),
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{"green", "test3"},
							WithID(NewRecordID("02")),
						),
					},
				),
				joined: NewFilter(
					NewAnd(
						NewPredicate(
							NewVariable("score"),
							&MoreThan{},
							NewVariable("const"),
						),
						NewPredicate(
							NewVariable("bike"),
							&Equal{},
							NewVariable("color"),
						),
					),
					NewDummyNode([]*Record{
						NewRecordFromSliceWithNormalize(
							fieldNames2,
							[]interface{}{"green", 7},
							WithID(NewRecordID("10")),
						),
						NewRecordFromSliceWithNormalize(
							fieldNames2,
							[]interface{}{"red", 5},
							WithID(NewRecordID("11")),
						),
						NewRecordFromSliceWithNormalize(
							fieldNames2,
							[]interface{}{"green", 4},
							WithID(NewRecordID("12")),
						),
						NewRecordFromSliceWithNormalize(
							fieldNames2,
							[]interface{}{"green", 2},
							WithID(NewRecordID("13")),
						),
					})),
			},
			want: NewDummyNode([]*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"bike", "name", "color", "score"},
					[]interface{}{"red", "test", "red", 5},
					WithID(NewRecordID("11")),
				),
				NewRecordFromSliceWithNormalize(
					fieldNames,
					[]interface{}{"blue", "test2"},
					WithID(NewRecordID("01")),
				),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"bike", "name", "color", "score"},
					[]interface{}{"green", "test3", "green", 7},
					WithID(NewRecordID("10")),
				),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"bike", "name", "color", "score"},
					[]interface{}{"green", "test3", "green", 4},
					WithID(NewRecordID("12")),
				),
			}),
			wantErr: false,
		},
		{
			name: "simple left join",
			fields: fields{
				maxJobCount: 2,
				isLeftJoin:  true,
				variables: map[octosql.VariableName]octosql.Value{
					octosql.NewVariableName("const"): octosql.MakeInt(3),
				},
				source: NewDummyNode(
					[]*Record{
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{"red", "test"},
							WithID(NewRecordID("00")),
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{"blue", "test2"},
							WithID(NewRecordID("01")),
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{"green", "test3"},
							WithID(NewRecordID("02")),
						),
					},
				),
				joined: NewFilter(
					NewAnd(
						NewPredicate(
							NewVariable("score"),
							&MoreThan{},
							NewVariable("const"),
						),
						NewPredicate(
							NewVariable("bike"),
							&Equal{},
							NewVariable("color"),
						),
					),
					NewDummyNode([]*Record{
						NewRecordFromSliceWithNormalize(
							fieldNames2,
							[]interface{}{"green", 7},
							WithID(NewRecordID("10")),
						),
						NewRecordFromSliceWithNormalize(
							fieldNames2,
							[]interface{}{"red", 5},
							WithID(NewRecordID("11")),
						),
						NewRecordFromSliceWithNormalize(
							fieldNames2,
							[]interface{}{"green", 4},
							WithID(NewRecordID("12")),
						),
						NewRecordFromSliceWithNormalize(
							fieldNames2,
							[]interface{}{"green", 2},
							WithID(NewRecordID("13")),
						),
					})),
			},
			want: NewDummyNode([]*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"bike", "name", "color", "score"},
					[]interface{}{"red", "test", "red", 5},
					WithID(NewRecordID("11")),
				),
				NewRecordFromSliceWithNormalize(
					fieldNames,
					[]interface{}{"blue", "test2"},
					WithID(NewRecordID("01")),
				),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"bike", "name", "color", "score"},
					[]interface{}{"green", "test3", "green", 7},
					WithID(NewRecordID("10")),
				),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"bike", "name", "color", "score"},
					[]interface{}{"green", "test3", "green", 4},
					WithID(NewRecordID("12")),
				),
			}),
			wantErr: false,
		},
		{
			name: "simple inner join",
			fields: fields{
				maxJobCount: 1,
				isLeftJoin:  false,
				variables: map[octosql.VariableName]octosql.Value{
					octosql.NewVariableName("const"): octosql.MakeInt(3),
				},
				source: NewDummyNode(
					[]*Record{
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{"red", "test"},
							WithID(NewRecordID("00")),
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{"blue", "test2"},
							WithID(NewRecordID("01")),
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{"green", "test3"},
							WithID(NewRecordID("02")),
						),
					},
				),
				joined: NewFilter(
					NewAnd(
						NewPredicate(
							NewVariable("score"),
							&MoreThan{},
							NewVariable("const"),
						),
						NewPredicate(
							NewVariable("bike"),
							&Equal{},
							NewVariable("color"),
						),
					),
					NewDummyNode([]*Record{
						NewRecordFromSliceWithNormalize(
							fieldNames2,
							[]interface{}{"green", 7},
							WithID(NewRecordID("10")),
						),
						NewRecordFromSliceWithNormalize(
							fieldNames2,
							[]interface{}{"red", 5},
							WithID(NewRecordID("11")),
						),
						NewRecordFromSliceWithNormalize(
							fieldNames2,
							[]interface{}{"green", 4},
							WithID(NewRecordID("12")),
						),
						NewRecordFromSliceWithNormalize(
							fieldNames2,
							[]interface{}{"green", 2},
							WithID(NewRecordID("13")),
						),
					})),
			},
			want: NewDummyNode([]*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"bike", "name", "color", "score"},
					[]interface{}{"red", "test", "red", 5},
					WithID(NewRecordID("11")),
				),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"bike", "name", "color", "score"},
					[]interface{}{"green", "test3", "green", 7},
					WithID(NewRecordID("10")),
				),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"bike", "name", "color", "score"},
					[]interface{}{"green", "test3", "green", 4},
					WithID(NewRecordID("12")),
				),
			}),
			wantErr: false,
		},
		{
			name: "empty stream",
			fields: fields{
				maxJobCount: 1,
				isLeftJoin:  true,
				variables: map[octosql.VariableName]octosql.Value{
					octosql.NewVariableName("const"): octosql.MakeInt(3),
				},
				source: NewDummyNode(nil),
				joined: NewDummyNode(nil),
			},
			want:    NewDummyNode(nil),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stateStorage := GetTestStorage(t)
			defer func() {
				go stateStorage.Close()
			}()
			tx := stateStorage.BeginTransaction()
			ctx := storage.InjectStateTransaction(context.Background(), tx)

			stream := NewLookupJoin(tt.fields.maxJobCount, stateStorage, tt.fields.source, tt.fields.joined, tt.fields.isLeftJoin)

			rs, _, err := stream.Get(ctx, tt.fields.variables, GetRawStreamID())
			if err != nil {
				t.Fatal("couldn't get actual record stream: ", err)
			}

			want, _, err := tt.want.Get(ctx, tt.fields.variables, GetRawStreamID())
			if err != nil {
				t.Fatal("couldn't get wanted record stream: ", err)
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			err = AreStreamsEqualNoOrdering(ctx, stateStorage, rs, want)
			if (err != nil) != tt.wantErr {
				t.Errorf("LookupJoin error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
