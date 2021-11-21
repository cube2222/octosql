package plugins

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

func TestPhysicalVariableContext(t *testing.T) {
	c := &physical.VariableContext{
		Fields: []physical.SchemaField{
			{
				Name: "test1",
				Type: octosql.String,
			},
			{
				Name: "test2",
				Type: octosql.Int,
			},
			{
				Name: "test3",
				Type: octosql.Boolean,
			},
		},
		Parent: &physical.VariableContext{
			Fields: []physical.SchemaField{
				{
					Name: "test4",
					Type: octosql.Time,
				},
				{
					Name: "test5",
					Type: octosql.Duration,
				},
				{
					Name: "test6",
					Type: octosql.Boolean,
				},
			},
			Parent: &physical.VariableContext{
				Fields: []physical.SchemaField{
					{
						Name: "test7",
						Type: octosql.Time,
					},
					{
						Name: "test8",
						Type: octosql.String,
					},
					{
						Name: "test9",
						Type: octosql.Null,
					},
				},
				Parent: nil,
			},
		},
	}

	outC := NativePhysicalVariableContextToProto(c).ToNativePhysicalVariableContext()

	assert.Equal(t, c, outC)
}
