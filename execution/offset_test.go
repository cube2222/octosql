package execution

import (
	"context"

	"github.com/cube2222/octosql"
	"testing"
)

func TestOffset_Get(t *testing.T) {
	ctx := context.Background()
	const NO_ERROR = ""

	tests := []struct {
		name       string
		vars       octosql.Variables
		node       *Offset
		wantStream *InMemoryStream
		wantError  string
	}{
		{
			name:       "negative offset value",
			vars:       octosql.NoVariables(),
			node:       NewOffset(NewDummyNode(nil), NewDummyValue(octosql.MakeInt(-42))),
			wantStream: nil,
			wantError:  "negative offset value",
		},
		{
			name:       "offset value not int",
			vars:       octosql.NoVariables(),
			node:       NewOffset(NewDummyNode(nil), NewDummyValue(octosql.MakeFloat(2.0))),
			wantStream: nil,
			wantError:  "offset value not int",
		},
		{
			name: "normal offset get",
			vars: octosql.NoVariables(),
			node: NewOffset(&DummyNode{
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
			}, NewDummyValue(octosql.MakeInt(2))),
			wantStream: NewInMemoryStream([]*Record{
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
			}),
			wantError: NO_ERROR,
		},
		{
			name: "offset bigger than number of rows",
			vars: octosql.NoVariables(),
			node: NewOffset(&DummyNode{
				[]*Record{
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{
							"num",
						},
						[]interface{}{
							1,
						}),
					NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{
							"num",
						},
						[]interface{}{
							2,
						}),
				},
			}, NewDummyValue(octosql.MakeInt(4))),
			wantStream: NewInMemoryStream([]*Record{}),
			wantError:  NO_ERROR,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs, err := tt.node.Get(ctx, tt.vars)

			if (err == nil) != (tt.wantError == NO_ERROR) {
				t.Errorf("exactly one of test.wantError, tt.node.Get() is not nil")
				return
			}

			if err != nil {
				if err.Error() != tt.wantError {
					t.Errorf("Unexpected error %v, wanted: %v", err.Error(), tt.wantError)
				}
				return
			}

			equal, err := AreStreamsEqual(context.Background(), rs, tt.wantStream)
			if !equal {
				t.Errorf("limitedStream doesn't work as expected")
			}
			if err != nil {
				t.Errorf("limitedStream comparison error: %v", err)
			}
		})
	}
}
