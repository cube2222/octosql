package execution

import (
	"testing"

	"github.com/cube2222/octosql"
)

func TestLeftJoinedStream_Next(t *testing.T) {
	fieldNames := []octosql.VariableName{
		octosql.NewVariableName("bike"),
		octosql.NewVariableName("name"),
	}
	fieldNames2 := []octosql.VariableName{
		octosql.NewVariableName("color"),
		octosql.NewVariableName("score"),
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
			name: "simple left join",
			fields: fields{
				variables: map[octosql.VariableName]octosql.Value{
					octosql.NewVariableName("const"): octosql.MakeInt(3),
				},
				source: NewInMemoryStream([]*Record{
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
				}, nil),
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
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"bike", "name", "color", "score"},
					[]interface{}{"red", "test", "red", 5},
				),
				NewRecordFromSliceWithNormalize(
					fieldNames,
					[]interface{}{"blue", "test2"},
				),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"bike", "name", "color", "score"},
					[]interface{}{"green", "test3", "green", 7},
				),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"bike", "name", "color", "score"},
					[]interface{}{"green", "test3", "green", 4},
				),
			}, nil),
			wantErr: false,
		},
		{
			name: "empty stream",
			fields: fields{
				variables: map[octosql.VariableName]octosql.Value{
					octosql.NewVariableName("const"): octosql.MakeInt(3),
				},
				source: NewInMemoryStream(nil, nil),
				joined: NewDummyNode(nil),
			},
			want:    NewInMemoryStream(nil, nil),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := &LeftJoinedStream{
				joiner: NewJoiner(
					2,
					tt.fields.variables,
					tt.fields.source,
					tt.fields.joined,
				),
			}
			equal, err := AreStreamsEqual(stream, tt.want)
			if (err != nil) != tt.wantErr {
				t.Errorf("LeftJoinedStream.Next() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && !equal {
				t.Errorf("LeftJoinedStream.Next() streams not equal")
			}
		})
	}
}
