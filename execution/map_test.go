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
				variables: map[octosql.VariableName]octosql.Value{
					octosql.NewVariableName("something_from_above"): octosql.MakeString("yeah"),
				},
				source: NewInMemoryStream(
					[]*Record{
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{3, "test1", "test2"},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{5, "test2", "test0"},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{4, "test4", "test5"},
						),
					},
				),
				keep: false,
			},
			want: NewInMemoryStream(
				[]*Record{
					NewRecordFromSliceWithNormalize(fieldNamesNew,
						[]interface{}{3, "yeah"},
					),
					NewRecordFromSliceWithNormalize(fieldNamesNew,
						[]interface{}{5, "yeah"},
					),
					NewRecordFromSliceWithNormalize(fieldNamesNew,
						[]interface{}{4, "yeah"},
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
				variables: map[octosql.VariableName]octosql.Value{
					octosql.NewVariableName("something_from_above"): octosql.MakeString("yeah"),
				},
				source: NewInMemoryStream(
					[]*Record{
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{3, "test1", "test2"},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{5, "test2", "test0"},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{4, "test4", "test5"},
						),
					},
				),
				keep: true,
			},
			want: NewInMemoryStream(
				[]*Record{
					NewRecordFromSliceWithNormalize(
						fieldNamesAll,
						[]interface{}{3, "yeah", 3, "test1", "test2"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNamesAll,
						[]interface{}{5, "yeah", 5, "test2", "test0"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNamesAll,
						[]interface{}{4, "yeah", 4, "test4", "test5"},
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
									NewRecordFromSliceWithNormalize(
										[]octosql.VariableName{octosql.NewVariableName("test")},
										[]interface{}{5},
									),
								},
							),
						),
					),
				},
				variables: map[octosql.VariableName]octosql.Value{},
				source: NewInMemoryStream(
					[]*Record{
						NewRecordFromSliceWithNormalize(
							[]octosql.VariableName{},
							[]interface{}{},
						),
					},
				),
				keep: false,
			},
			want: NewInMemoryStream(
				[]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{octosql.NewVariableName("awesome")},
						[]interface{}{5},
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
									NewRecordFromSliceWithNormalize(
										[]octosql.VariableName{
											octosql.NewVariableName("test"),
											octosql.NewVariableName("test"),
										},
										[]interface{}{
											5,
											3,
										},
									),
								},
							),
						),
					),
				},
				variables: map[octosql.VariableName]octosql.Value{},
				source: NewInMemoryStream(
					[]*Record{
						NewRecordFromSliceWithNormalize(
							[]octosql.VariableName{},
							[]interface{}{},
						),
					},
				),
				keep: false,
			},
			want: NewInMemoryStream(
				[]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{},
						[]interface{}{},
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
