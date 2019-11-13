package execution

import (
	"github.com/cube2222/octosql"
	"testing"
)

func TestLimit_Get(t *testing.T) {
	const NO_ERROR = ""

	tests := []struct {
		name       string
		vars       octosql.Variables
		node       *Limit
		wantStream *InMemoryStream
		wantError  string
	}{
		{
			name:       "negative limit value",
			vars:       octosql.NoVariables(),
			node:       NewLimit(NewDummyNode(nil), NewDummyValue(octosql.MakeInt(-42))),
			wantStream: nil,
			wantError:  "negative limit value",
		},
		{
			name:       "limit value not int",
			vars:       octosql.NoVariables(),
			node:       NewLimit(NewDummyNode(nil), NewDummyValue(octosql.MakeFloat(2.0))),
			wantStream: nil,
			wantError:  "limit value not int",
		},
		{
			name: "normal limit get",
			vars: octosql.NoVariables(),
			node: NewLimit(&DummyNode{
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
			}, NewDummyValue(octosql.MakeInt(3))),
			wantStream: NewInMemoryStream([]*Record{
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
			}, nil),
			wantError: NO_ERROR,
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
			wantStream: NewInMemoryStream([]*Record{}, nil),
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
				t.Errorf("limitedStream doesn't work as expected")
			}
			if err != nil {
				t.Errorf("limitedStream comparison error: %v", err)
			}
		})
	}
}

func TestLimitedStream_Next(t *testing.T) {
	tests := []struct {
		name       string
		stream     *LimitedStream
		wantStream *InMemoryStream
	}{
		{
			name: "Simplest limitedStream possible",
			stream: newLimitedStream(
				NewInMemoryStream([]*Record{
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
				}, nil),
				1,
			),
			wantStream: NewInMemoryStream([]*Record{
				NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{
						"num",
					},
					[]interface{}{
						1,
					}),
			}, nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			equal, err := AreStreamsEqual(tt.stream, tt.wantStream)
			if !equal {
				t.Errorf("limitedStream doesn't work as intended")
			}
			if err != nil {
				t.Errorf("limitedStream comparison error: %v", err)
			}
		})
	}
}
