package execution

import (
	"testing"

	"github.com/cube2222/octosql"
)

func TestFilteredStream_Next(t *testing.T) {
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
				variables: map[octosql.VariableName]interface{}{
					octosql.NewVariableName("const"): 3,
				},
				source: NewInMemoryStream(
					[]*Record{
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{5, "test"},
						),
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{4, "test2"},
						),
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{3, "test3"},
						),
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{3, "test33"},
						),
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{2, "test2"},
						),
					},
				),
			},
			want: NewInMemoryStream(
				[]*Record{
					NewRecordFromSlice(
						fieldNames,
						octosql.Tuple{5, "test"},
					),
					NewRecordFromSlice(
						fieldNames,
						octosql.Tuple{4, "test2"},
					),
					NewRecordFromSlice(
						fieldNames,
						octosql.Tuple{2, "test2"},
					),
				},
			),
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
				variables: map[octosql.VariableName]interface{}{
					octosql.NewVariableName("const"): 3,
				},
				source: NewInMemoryStream(
					[]*Record{
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{5, "test"},
						),
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{5, "test"},
						),
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{4, "test2"},
						),
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{3, "test3"},
						),
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{3, "test33"},
						),
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{2, "test2"},
						),
						NewRecordFromSlice(
							fieldNames,
							octosql.Tuple{2, "test2"},
						),
					},
				),
			},
			want: NewInMemoryStream(
				[]*Record{
					NewRecordFromSlice(
						fieldNames,
						octosql.Tuple{5, "test"},
					),
					NewRecordFromSlice(
						fieldNames,
						octosql.Tuple{5, "test"},
					),
					NewRecordFromSlice(
						fieldNames,
						octosql.Tuple{4, "test2"},
					),
					NewRecordFromSlice(
						fieldNames,
						octosql.Tuple{2, "test2"},
					),
					NewRecordFromSlice(
						fieldNames,
						octosql.Tuple{2, "test2"},
					),
				},
			),
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
				variables: map[octosql.VariableName]interface{}{
					octosql.NewVariableName("const"): 3,
				},
				source: NewInMemoryStream(nil),
			},
			want:    NewInMemoryStream(nil),
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
			equal, err := AreStreamsEqual(stream, tt.want)
			if (err != nil) != tt.wantErr {
				t.Errorf("FilteredStream.Next() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && !equal {
				t.Errorf("FilteredStream.Next() streams not equal")
			}
		})
	}
}
