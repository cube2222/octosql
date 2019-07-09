package execution

import (
	"testing"

	"github.com/cube2222/octosql"
)

func TestOrderBy_Get(t *testing.T) {
	type args struct {
		stream RecordStream
		fields []OrderField
	}
	tests := []struct {
		name    string
		args    args
		want    RecordStream
		wantErr bool
	}{
		{
			name: "simple order - one column int ascending",
			args: args{
				stream: NewInMemoryStream([]*Record{
					NewRecordFromSlice(
						[]octosql.VariableName{"id", "age"},
						octosql.Tuple{1, 7}),
					NewRecordFromSlice(
						[]octosql.VariableName{"id", "age"},
						octosql.Tuple{2, 10}),
					NewRecordFromSlice(
						[]octosql.VariableName{"id", "age"},
						octosql.Tuple{3, 2}),
				}),
				fields: []OrderField{
					{
						ColumnName: "age",
						Direction:  Ascending,
					},
				},
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSlice(
					[]octosql.VariableName{"id", "age"},
					octosql.Tuple{3, 2}),
				NewRecordFromSlice(
					[]octosql.VariableName{"id", "age"},
					octosql.Tuple{1, 7}),
				NewRecordFromSlice(
					[]octosql.VariableName{"id", "age"},
					octosql.Tuple{2, 10}),
			}),
			wantErr: false,
		},

		{
			name: "simple order - one column string descending",
			args: args{
				stream: NewInMemoryStream([]*Record{
					NewRecordFromSlice(
						[]octosql.VariableName{"name", "age"},
						octosql.Tuple{"b", 7}),
					NewRecordFromSlice(
						[]octosql.VariableName{"name", "age"},
						octosql.Tuple{"c", 10}),
					NewRecordFromSlice(
						[]octosql.VariableName{"name", "age"},
						octosql.Tuple{"a", 2}),
				}),
				fields: []OrderField{
					{
						ColumnName: "name",
						Direction:  Descending,
					},
				},
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSlice(
					[]octosql.VariableName{"name", "age"},
					octosql.Tuple{"c", 10}),
				NewRecordFromSlice(
					[]octosql.VariableName{"name", "age"},
					octosql.Tuple{"b", 7}),
				NewRecordFromSlice(
					[]octosql.VariableName{"name", "age"},
					octosql.Tuple{"a", 2}),
			}),
			wantErr: false,
		},

		{
			name: "complex order - string ascending then int descending",
			args: args{
				stream: NewInMemoryStream([]*Record{
					NewRecordFromSlice(
						[]octosql.VariableName{"name", "age"},
						octosql.Tuple{"a", 7}),
					NewRecordFromSlice(
						[]octosql.VariableName{"name", "age"},
						octosql.Tuple{"d", 19}),
					NewRecordFromSlice(
						[]octosql.VariableName{"name", "age"},
						octosql.Tuple{"a", -2}),
					NewRecordFromSlice(
						[]octosql.VariableName{"name", "age"},
						octosql.Tuple{"c", 1}),
					NewRecordFromSlice(
						[]octosql.VariableName{"name", "age"},
						octosql.Tuple{"d", 17}),
				}),
				fields: []OrderField{
					{
						ColumnName: "name",
						Direction:  Ascending,
					},
					{
						ColumnName: "age",
						Direction:  Descending,
					},
				},
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSlice(
					[]octosql.VariableName{"name", "age"},
					octosql.Tuple{"a", 7}),
				NewRecordFromSlice(
					[]octosql.VariableName{"name", "age"},
					octosql.Tuple{"a", -2}),
				NewRecordFromSlice(
					[]octosql.VariableName{"name", "age"},
					octosql.Tuple{"c", 1}),
				NewRecordFromSlice(
					[]octosql.VariableName{"name", "age"},
					octosql.Tuple{"d", 19}),
				NewRecordFromSlice(
					[]octosql.VariableName{"name", "age"},
					octosql.Tuple{"d", 17}),
			}),
			wantErr: false,
		},

		{
			name: "failed - missing field",
			args: args{
				stream: NewInMemoryStream([]*Record{
					NewRecordFromSlice(
						[]octosql.VariableName{"name", "age"},
						octosql.Tuple{"a", 7}),
					NewRecordFromSlice(
						[]octosql.VariableName{"name", "age?"},
						octosql.Tuple{"d", 19}),
				}),
				fields: []OrderField{
					{
						ColumnName: "age",
						Direction:  Descending,
					},
				},
			},
			want:    nil,
			wantErr: true,
		},

		{
			name: "failed - type mismatch",
			args: args{
				stream: NewInMemoryStream([]*Record{
					NewRecordFromSlice(
						[]octosql.VariableName{"name", "age"},
						octosql.Tuple{"a", 7}),
					NewRecordFromSlice(
						[]octosql.VariableName{"name", "age"},
						octosql.Tuple{"d", 19.5}),
				}),
				fields: []OrderField{
					{
						ColumnName: "age",
						Direction:  Descending,
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "failed - non sortable boolean",
			args: args{
				stream: NewInMemoryStream([]*Record{
					NewRecordFromSlice(
						[]octosql.VariableName{"is_nice"},
						octosql.Tuple{false}),
					NewRecordFromSlice(
						[]octosql.VariableName{"is_nice"},
						octosql.Tuple{true}),
				}),
				fields: []OrderField{
					{
						ColumnName: "is_nice",
						Direction:  Descending,
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ordered, err := createOrderedStream(tt.args.fields, tt.args.stream)
			if err != nil && !tt.wantErr {
				t.Errorf("Error in create stream")
				return
			} else if err != nil {
				return
			}

			equal, err := AreStreamsEqual(tt.want, ordered)
			if err != nil {
				t.Errorf("Error in AreStreamsEqual()")
				return
			}

			if !equal {
				t.Errorf("Streams don't match")
				return
			}
		})
	}
}
