package execution

import (
	"context"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestLimit_Get(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		vars      octosql.Variables
		node      *Limit
		want      Node
		wantError bool
	}{
		{
			name:      "negative limit value",
			vars:      octosql.NoVariables(),
			node:      NewLimit(NewDummyNode(nil), NewDummyValue(octosql.MakeInt(-42))),
			want:      nil,
			wantError: true,
		},
		{
			name:      "limit value not int",
			vars:      octosql.NoVariables(),
			node:      NewLimit(NewDummyNode(nil), NewDummyValue(octosql.MakeFloat(2.0))),
			want:      nil,
			wantError: true,
		},
		{
			name: "normal limit get",
			vars: octosql.NoVariables(),
			node: NewLimit(NewDummyNode(
				[]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{
							"num",
						},
						[]interface{}{
							1e10,
						}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{
							"num",
						},
						[]interface{}{
							3.21,
						}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{
							"flag",
						},
						[]interface{}{
							false,
						}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{
							"num",
						},
						[]interface{}{
							2.23e7,
						}),
				},
			), NewDummyValue(octosql.MakeInt(3))),
			want: NewDummyNode([]*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{
						"num",
					},
					[]interface{}{
						1e10,
					}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{
						"num",
					},
					[]interface{}{
						3.21,
					}),
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{
						"flag",
					},
					[]interface{}{
						false,
					}),
			}),
			wantError: false,
		},
		{
			name: "zero limit get",
			vars: octosql.NoVariables(),
			node: NewLimit(&DummyNode{
				[]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{
							"num",
						},
						[]interface{}{
							1,
						}),
				},
			}, NewDummyValue(octosql.MakeInt(0))),
			want:      NewDummyNode([]*Record{}),
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stateStorage := storage.GetTestStorage(t)

			tx := stateStorage.BeginTransaction()
			ctx := storage.InjectStateTransaction(ctx, tx)

			rs, _, err := tt.node.Get(ctx, tt.vars, GetRawStreamID())
			if (err != nil) != tt.wantError {
				t.Errorf("Unexpected error %v, wanted: %v", err, tt.wantError)
				return
			} else if tt.wantError {
				return
			}

			want, _, err := tt.want.Get(ctx, tt.vars, GetRawStreamID())
			if err != nil {
				t.Fatal("couldn't get wanted record stream: ", err)
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			err = AreStreamsEqualNoOrdering(ctx, stateStorage, rs, want)
			if err != nil {
				t.Errorf("limitedStream comparison error: %v", err)
			}

			if err := rs.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close limited stream: %v", err)
				return
			}
		})
	}
}
