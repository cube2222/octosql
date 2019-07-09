package execution

import (
	"testing"

	"github.com/cube2222/octosql"
)

func TestInnerJoinedStream_Next(t *testing.T) {
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
			name: "simple inner join",
			fields: fields{
				variables: map[octosql.VariableName]interface{}{
					octosql.NewVariableName("const"): 3,
				},
				source: NewInMemoryStream(
					[]*Record{
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{"red", "test"},
						),
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{"blue", "test2"},
						),
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{"green", "test3"},
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
						NewRecordFromSlice(
							fieldNames2,
							octosql.Tuple{"green", 7},
						),
						NewRecordFromSlice(
							fieldNames2,
							octosql.Tuple{"red", 5},
						),
						NewRecordFromSlice(
							fieldNames2,
							octosql.Tuple{"green", 4},
						),
						NewRecordFromSlice(
							fieldNames2,
							octosql.Tuple{"green", 2},
						),
					})),
			},
			want: NewInMemoryStream(
				[]*Record{
					NewRecordFromSlice(
						[]octosql.VariableName{"bike", "name", "color", "score"},
						octosql.Tuple{"red", "test", "red", 5},
					),
					NewRecordFromSlice(
						[]octosql.VariableName{"bike", "name", "color", "score"},
						octosql.Tuple{"green", "test3", "green", 7},
					),
					NewRecordFromSlice(
						[]octosql.VariableName{"bike", "name", "color", "score"},
						octosql.Tuple{"green", "test3", "green", 4},
					),
				},
			),
			wantErr: false,
		},
		{
			name: "empty stream",
			fields: fields{
				variables: map[octosql.VariableName]interface{}{
					octosql.NewVariableName("const"): 3,
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
				variables: tt.fields.variables,
				source:    tt.fields.source,
				joined:    tt.fields.joined,
			}
			equal, err := AreStreamsEqual(stream, tt.want)
			if (err != nil) != tt.wantErr {
				t.Errorf("InnerJoinedStream.Next() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && !equal {
				t.Errorf("InnerJoinedStream.Next() streams not equal")
			}
		})
	}
}
