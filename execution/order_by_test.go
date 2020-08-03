package execution

import (
	"context"
	"testing"
	"time"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/storage"
)

func TestOrderBy_Get(t *testing.T) {
	now := time.Now()

	start := time.Date(2020, 7, 2, 14, 0, 0, 0, time.UTC)
	firstWindow := start
	secondWindow := start.Add(time.Minute * 10)
	thirdWindow := start.Add(time.Minute * 20)

	type args struct {
		source      Node
		expressions []Expression
		directions  []OrderDirection
		eventTimeField octosql.VariableName
	}
	tests := []struct {
		name    string
		args    args
		want    []*Record
		wantErr bool
	}{
		{
			name: "simple order - one column int ascending",
			args: args{
				source: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{1, 7}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{2, 10}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "age"},
						[]interface{}{3, 2}),
				}),
				expressions: []Expression{NewVariable(octosql.NewVariableName("age"))},
				directions:  []OrderDirection{Ascending},
				eventTimeField: octosql.NewVariableName(""),
			},
			want: []*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{3, 2}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{1, 7}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "age"},
					[]interface{}{2, 10}),
			},
			wantErr: false,
		},
		{
			name: "simple order - one column string descending",
			args: args{
				source: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age"},
						[]interface{}{"ba", 10}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age"},
						[]interface{}{"c", 7}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age"},
						[]interface{}{"b", 2}),
				}),
				expressions: []Expression{NewVariable(octosql.NewVariableName("name"))},
				directions:  []OrderDirection{Descending},
				eventTimeField: octosql.NewVariableName(""),
			},
			want: []*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "age"},
					[]interface{}{"c", 7}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "age"},
					[]interface{}{"ba", 10}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "age"},
					[]interface{}{"b", 2}),
			},
			wantErr: false,
		},
		{
			name: "simple order - one column time descending",
			args: args{
				source: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "birth"},
						[]interface{}{"b", now}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "birth"},
						[]interface{}{"c", now.Add(time.Hour)}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "birth"},
						[]interface{}{"a", now.Add(-1 * time.Hour)}),
				}),
				expressions: []Expression{NewVariable(octosql.NewVariableName("birth"))},
				directions:  []OrderDirection{Descending},
				eventTimeField: octosql.NewVariableName(""),
			},
			want: []*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "birth"},
					[]interface{}{"c", now.Add(time.Hour)}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "birth"},
					[]interface{}{"b", now}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "birth"},
					[]interface{}{"a", now.Add(-1 * time.Hour)}),
			},
			wantErr: false,
		},
		{
			name: "complex order - string ascending then int descending",
			args: args{
				source: NewDummyNode([]*Record{
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
				}),
				expressions: []Expression{
					NewVariable(octosql.NewVariableName("name")),
					NewVariable(octosql.NewVariableName("age")),
				},
				directions: []OrderDirection{
					Ascending,
					Descending,
				},
				eventTimeField: octosql.NewVariableName(""),
			},
			want: []*Record{
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
			},
			wantErr: false,
		},
		{
			name: "complex order - string ascending then float descending",
			args: args{
				source: NewDummyNode([]*Record{
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
				}),
				expressions: []Expression{
					NewVariable("name"),
					NewFunctionExpression(&FuncIdentity, []Expression{NewVariable("age")}),
				},
				directions: []OrderDirection{
					Ascending,
					Descending,
				},
				eventTimeField: octosql.NewVariableName(""),
			},

			want: []*Record{
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
			},
			wantErr: false,
		},
		{
			name: "complex order - simple retraction",
			args: args{
				source: NewDummyNode([]*Record{
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
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "age"},
						[]interface{}{"d", 19.02},  WithUndo()),
				}),
				expressions: []Expression{
					NewVariable("name"),
					NewFunctionExpression(&FuncIdentity, []Expression{NewVariable("age")}),
				},
				directions: []OrderDirection{
					Ascending,
					Descending,
				},
				eventTimeField: octosql.NewVariableName(""),
			},

			want: []*Record{
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
					[]interface{}{"d", 17.918}),
			},
			wantErr: false,
		},
		{
			name: "simple order - event time",

			args: args{
				source: NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "t"},
						[]interface{}{"a", firstWindow},
						WithEventTimeField(octosql.NewVariableName("t"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "t"},
						[]interface{}{"c", firstWindow},
						WithEventTimeField(octosql.NewVariableName("t"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "t"},
						[]interface{}{"b", secondWindow},
						WithEventTimeField(octosql.NewVariableName("t"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "t"},
						[]interface{}{"a", secondWindow},
						WithEventTimeField(octosql.NewVariableName("t"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "t"},
						[]interface{}{"c", thirdWindow},
						WithEventTimeField(octosql.NewVariableName("t"))),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"name", "t"},
						[]interface{}{"a", thirdWindow},
						WithEventTimeField(octosql.NewVariableName("t"))),
				}),
				expressions: []Expression{
					NewVariable(octosql.NewVariableName("t")),
					NewVariable(octosql.NewVariableName("name")),
				},
				directions:  []OrderDirection{Ascending, Descending},
				eventTimeField: octosql.NewVariableName("t"),
			},

			want: []*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "t"},
					[]interface{}{"c", firstWindow},
					WithEventTimeField(octosql.NewVariableName("t"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "t"},
					[]interface{}{"a", firstWindow},
					WithEventTimeField(octosql.NewVariableName("t"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "t"},
					[]interface{}{"b", secondWindow},
					WithEventTimeField(octosql.NewVariableName("t"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "t"},
					[]interface{}{"a", secondWindow},
					WithEventTimeField(octosql.NewVariableName("t"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "t"},
					[]interface{}{"c", thirdWindow},
					WithEventTimeField(octosql.NewVariableName("t"))),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "t"},
					[]interface{}{"a", thirdWindow},
					WithEventTimeField(octosql.NewVariableName("t"))),
		},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			stateStorage := storage.GetTestStorage(t)

			ctx := context.Background()
			ob := NewOrderBy(
				stateStorage,
				tt.args.source,
				tt.args.expressions,
				tt.args.directions,
				tt.args.eventTimeField,
				NewWatermarkTrigger(),
			)

			expectedOutput := tt.want

			stream := GetTestStream(t, stateStorage, octosql.NoVariables(), ob)

			tx := stateStorage.BeginTransaction()
			want := NewInMemoryStream(storage.InjectStateTransaction(context.Background(), tx), expectedOutput)
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			err := AreStreamsEqualWithOrdering(ctx, stateStorage, want, stream)
			if (err != nil) == (tt.wantErr == false) {
				t.Fatal(err)
			}

			if err := stream.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close group_by stream: %v", err)
				return
			}
			if err := want.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close wanted in_memory stream: %v", err)
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
