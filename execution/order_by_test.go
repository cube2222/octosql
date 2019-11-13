package execution

import (
	"testing"
	"time"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
)

func TestOrderBy_Get(t *testing.T) {
	now := time.Now()

	type args struct {
		stream      RecordStream
		expressions []Expression
		directions  []OrderDirection
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
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{1, 7}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{2, 10}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{3, 2}),
				}, nil),
				expressions: []Expression{NewVariable(octosql.NewVariableName("age"))},
				directions:  []OrderDirection{Ascending},
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{3, 2}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{1, 7}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{2, 10}),
			}, nil),
			wantErr: false,
		},
		{
			name: "simple order - one column string descending",
			args: args{
				stream: NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age"},
						[]interface{}{"b", 10}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age"},
						[]interface{}{"c", 7}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age"},
						[]interface{}{"a", 2}),
				}, nil),
				expressions: []Expression{NewVariable(octosql.NewVariableName("name"))},
				directions:  []OrderDirection{Descending},
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "age"},
					[]interface{}{"c", 7}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "age"},
					[]interface{}{"b", 10}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "age"},
					[]interface{}{"a", 2}),
			}, nil),
			wantErr: false,
		},
		{
			name: "simple order - one column time descending",
			args: args{
				stream: NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "birth"},
						[]interface{}{"b", now}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "birth"},
						[]interface{}{"c", now.Add(time.Hour)}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "birth"},
						[]interface{}{"a", now.Add(-1 * time.Hour)}),
				}, nil),
				expressions: []Expression{NewVariable(octosql.NewVariableName("birth"))},
				directions:  []OrderDirection{Descending},
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "birth"},
					[]interface{}{"c", now.Add(time.Hour)}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "birth"},
					[]interface{}{"b", now}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "birth"},
					[]interface{}{"a", now.Add(-1 * time.Hour)}),
			}, nil),
			wantErr: false,
		},
		{
			name: "complex order - string ascending then int descending",
			args: args{
				stream: NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age"},
						[]interface{}{"a", 7}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age"},
						[]interface{}{"d", 19}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age"},
						[]interface{}{"a", -2}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age"},
						[]interface{}{"c", 1}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age"},
						[]interface{}{"d", 17}),
				}, nil),
				expressions: []Expression{
					NewVariable(octosql.NewVariableName("name")),
					NewVariable(octosql.NewVariableName("age")),
				},
				directions: []OrderDirection{
					Ascending,
					Descending,
				},
			},
			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "age"},
					[]interface{}{"a", 7}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "age"},
					[]interface{}{"a", -2}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "age"},
					[]interface{}{"c", 1}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "age"},
					[]interface{}{"d", 19}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "age"},
					[]interface{}{"d", 17}),
			}, nil),
			wantErr: false,
		},
		{
			name: "complex order - string ascending then int descending",
			args: args{
				stream: NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age"},
						[]interface{}{"a", 7.3}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age"},
						[]interface{}{"d", 19.02}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age"},
						[]interface{}{"a", -2.248}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age"},
						[]interface{}{"c", 1.123}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age"},
						[]interface{}{"d", 17.918}),
				}, nil),
				expressions: []Expression{
					NewVariable("name"),
					NewFunctionExpression(&FuncIdentity, []Expression{NewVariable("age")}),
				},
				directions: []OrderDirection{
					Ascending,
					Descending,
				},
			},

			want: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "age"},
					[]interface{}{"a", 7.3}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "age"},
					[]interface{}{"a", -2.248}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "age"},
					[]interface{}{"c", 1.123}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "age"},
					[]interface{}{"d", 19.02}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "age"},
					[]interface{}{"d", 17.918}),
			}, nil),
			wantErr: false,
		},
		{
			name: "failed - missing field",
			args: args{
				stream: NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age"},
						[]interface{}{"a", 7}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age?"},
						[]interface{}{"d", 19}),
				}, nil),
				expressions: []Expression{NewVariable(octosql.NewVariableName("age"))},
				directions:  []OrderDirection{Descending},
			},
			want:    nil,
			wantErr: true,
		},

		{
			name: "failed - type mismatch",
			args: args{
				stream: NewInMemoryStream([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age"},
						[]interface{}{"a", 7}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age"},
						[]interface{}{"d", 19.5}),
				}, nil),
				expressions: []Expression{NewVariable(octosql.NewVariableName("age"))},
				directions:  []OrderDirection{Descending},
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ordered, err := createOrderedStream(tt.args.expressions, tt.args.directions, octosql.NoVariables(), tt.args.stream)
			if err != nil && !tt.wantErr {
				t.Errorf("Error in create stream: %v", err)
				return
			} else if err != nil {
				return
			}

			equal, err := AreStreamsEqual(tt.want, ordered)
			if err != nil {
				t.Errorf("Error in AreStreamsEqual(): %v", err)
				return
			}

			if !equal {
				t.Errorf("Streams don't match")
				return
			}
		})
	}
}

type AnyOk struct {
}

func (*AnyOk) Document() docs.Documentation {
	panic("implement me")
}
func (*AnyOk) Validate(args ...octosql.Value) error {
	return nil
}

var FuncIdentity = Function{
	Validator: &AnyOk{},
	Logic: func(args ...octosql.Value) (octosql.Value, error) {
		return args[0], nil
	},
}
