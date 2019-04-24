package execution

import (
	"github.com/cube2222/octosql"
	"testing"
)

func TestLimitedStream_Next(t *testing.T) {
	tests := []struct {
		name       string
		stream     *limitedStream
		wantStream *InMemoryStream
	}{
		{
			name: "Simplest limitedStream possible",
			stream: newLimitedStream(
				NewInMemoryStream([]*Record{
					UtilNewRecord([]octosql.VariableName{
						"num",
					},
						[]interface{}{
							1,
						}),
					UtilNewRecord([]octosql.VariableName{
						"num",
					},
						[]interface{}{
							2,
						}),
				}),
				1,
			),
			wantStream: NewInMemoryStream([]*Record{
				UtilNewRecord([]octosql.VariableName{
					"num",
				},
					[]interface{}{
						1,
					}),
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			equal, err := AreStreamsEqual(tt.stream, tt.wantStream)
			if !equal {
				t.Errorf("limitedStream doesn't work as expected")
			}
			if err != nil {
				t.Errorf("limitedStream error: %v", err)
			}
		})
	}
}
