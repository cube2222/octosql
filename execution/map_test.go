package execution

import (
	"testing"

	"github.com/cube2222/octosql"
)

func TestMappedStream_Next(t *testing.T) {
	fieldNames := []octosql.VariableName{
		octosql.NewVariableName("age"),
		octosql.NewVariableName("something"),
		octosql.NewVariableName("something_else"),
	}

	fieldNamesNew := []octosql.VariableName{
		octosql.NewVariableName("awesome_age"),
		octosql.NewVariableName("awesome_something"),
	}

	fieldNamesAll := append(fieldNamesNew, fieldNames...)

	type fields struct {
		expressions []NamedExpression
		variables   octosql.Variables
		source      RecordStream
		keep        bool
	}
	tests := []struct {
		name    string
		fields  fields
		want    RecordStream
		wantErr bool
	}{
		{
			name: "map without keep",
			fields: fields{
				expressions: []NamedExpression{
					NewAliasedExpression(
						octosql.NewVariableName("awesome_age"),
						NewVariable(octosql.NewVariableName("age")),
					),
					NewAliasedExpression(
						octosql.NewVariableName("awesome_something"),
						NewVariable(octosql.NewVariableName("something_from_above")),
					),
				},
				variables: map[octosql.VariableName]interface{}{
					octosql.NewVariableName("something_from_above"): "yeah",
				},
				source: NewInMemoryStream(
					[]*Record{
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{3, "test1", "test2"},
						),
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{5, "test2", "test0"},
						),
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{4, "test4", "test5"},
						),
					},
				),
				keep: false,
			},
			want: NewInMemoryStream(
				[]*Record{
					NewRecordFromSlice(fieldNamesNew,
						octosql.Tuple{3, "yeah"},
					),
					NewRecordFromSlice(fieldNamesNew,
						octosql.Tuple{5, "yeah"},
					),
					NewRecordFromSlice(fieldNamesNew,
						octosql.Tuple{4, "yeah"},
					),
				},
			),
			wantErr: false,
		},
		{
			name: "map with keep",
			fields: fields{
				expressions: []NamedExpression{
					NewAliasedExpression(
						octosql.NewVariableName("awesome_age"),
						NewVariable(octosql.NewVariableName("age")),
					),
					NewAliasedExpression(
						octosql.NewVariableName("awesome_something"),
						NewVariable(octosql.NewVariableName("something_from_above")),
					),
				},
				variables: map[octosql.VariableName]interface{}{
					octosql.NewVariableName("something_from_above"): "yeah",
				},
				source: NewInMemoryStream(
					[]*Record{
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{3, "test1", "test2"},
						),
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{5, "test2", "test0"},
						),
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{4, "test4", "test5"},
						),
					},
				),
				keep: true,
			},
			want: NewInMemoryStream(
				[]*Record{
					NewRecordFromSlice(
						fieldNamesAll,
						octosql.Tuple{3, "yeah", 3, "test1", "test2"},
					),
					NewRecordFromSlice(
						fieldNamesAll,
						octosql.Tuple{5, "yeah", 5, "test2", "test0"},
					),
					NewRecordFromSlice(
						fieldNamesAll,
						octosql.Tuple{4, "yeah", 4, "test4", "test5"},
					),
				},
			),
			wantErr: false,
		},
		{
			name: "map with subquery",
			fields: fields{
				expressions: []NamedExpression{
					NewAliasedExpression(
						octosql.NewVariableName("awesome"),
						NewNodeExpression(
							NewDummyNode(
								[]*Record{
									NewRecordFromSlice(
										[]octosql.VariableName{octosql.NewVariableName("test")},
										octosql.Tuple{5},
									),
								},
							),
						),
					),
				},
				variables: map[octosql.VariableName]interface{}{},
				source: NewInMemoryStream(
					[]*Record{
						NewRecordFromSlice(
							[]octosql.VariableName{},
							octosql.Tuple{},
						),
					},
				),
				keep: false,
			},
			want: NewInMemoryStream(
				[]*Record{
					NewRecordFromSlice(
						[]octosql.VariableName{octosql.NewVariableName("awesome")},
						octosql.Tuple{5},
					),
				},
			),
			wantErr: false,
		},
		{
			name: "map with invalid subquery",
			fields: fields{
				expressions: []NamedExpression{
					NewAliasedExpression(
						octosql.NewVariableName("awesome"),
						NewNodeExpression(
							NewDummyNode(
								[]*Record{
									NewRecordFromSlice(
										[]octosql.VariableName{
											octosql.NewVariableName("test"),
											octosql.NewVariableName("test"),
										},
										octosql.Tuple{
											5,
											3,
										},
									),
								},
							),
						),
					),
				},
				variables: map[octosql.VariableName]interface{}{},
				source: NewInMemoryStream(
					[]*Record{
						NewRecordFromSlice(
							[]octosql.VariableName{},
							octosql.Tuple{},
						),
					},
				),
				keep: false,
			},
			want: NewInMemoryStream(
				[]*Record{
					NewRecordFromSlice(
						[]octosql.VariableName{},
						octosql.Tuple{},
					),
				},
			),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := &MappedStream{
				expressions: tt.fields.expressions,
				variables:   tt.fields.variables,
				source:      tt.fields.source,
				keep:        tt.fields.keep,
			}
			equal, err := AreStreamsEqual(stream, tt.want)
			if (err != nil) != tt.wantErr {
				t.Errorf("MappedStream.Next() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && !equal {
				t.Errorf("MappedStream.Next() streams not equal")
			}
		})
	}
}
