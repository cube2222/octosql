package execution

import (
	"github.com/cube2222/octosql"
	"testing"
)

func TestOffset_Get(t *testing.T) {
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
			node:       NewOffset(NewDummyNode(nil), NewDummyValue(-42)),
			wantStream: nil,
			wantError:  "negative offset value",
		},
		{
			name:       "offset value not int",
			vars:       octosql.NoVariables(),
			node:       NewOffset(NewDummyNode(nil), NewDummyValue(2.0)),
			wantStream: nil,
			wantError:  "offset value not int",
		},
		{
			name: "normal offset get",
			vars: octosql.NoVariables(),
			node: NewOffset(&DummyNode{
				[]*Record{
					NewRecordFromSlice(
						[]octosql.VariableName{
							"num",
						},
						octosql.Tuple{
							1e10,
						}),
					NewRecordFromSlice(
						[]octosql.VariableName{
							"num",
						},
						octosql.Tuple{
							3.21,
						}),
					NewRecordFromSlice(
						[]octosql.VariableName{
							"flag",
						},
						octosql.Tuple{
							false,
						}),
					NewRecordFromSlice(
						[]octosql.VariableName{
							"num",
						},
						octosql.Tuple{
							2.23e7,
						}),
				},
			}, &DummyValue{2}),
			wantStream: NewInMemoryStream([]*Record{
				NewRecordFromSlice(
					[]octosql.VariableName{
						"flag",
					},
					octosql.Tuple{
						false,
					}),
				NewRecordFromSlice(
					[]octosql.VariableName{
						"num",
					},
					octosql.Tuple{
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
					NewRecordFromSlice(
						[]octosql.VariableName{
							"num",
						},
						octosql.Tuple{
							1,
						}),
					NewRecordFromSlice(
						[]octosql.VariableName{
							"num",
						},
						octosql.Tuple{
							2,
						}),
				},
			}, &DummyValue{4}),
			wantStream: NewInMemoryStream([]*Record{}),
			wantError:  NO_ERROR,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs, err := tt.node.Get(tt.vars)

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

			equal, err := AreStreamsEqual(rs, tt.wantStream)
			if !equal {
				t.Errorf("LimitedStream doesn't work as expected")
			}
			if err != nil {
				t.Errorf("LimitedStream comparison error: %v", err)
			}
		})
	}
}
