package plugins

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cube2222/octosql/execution"
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

func TestExecutionVariableContext(t *testing.T) {
	c := &execution.VariableContext{
		Values: []octosql.Value{
			octosql.NewString("test1"),
			octosql.NewInt(42),
			octosql.NewBoolean(true),
		},
		Parent: &execution.VariableContext{
			Values: []octosql.Value{
				octosql.NewDuration(time.Second * 3),
				octosql.NewBoolean(false),
			},
			Parent: &execution.VariableContext{
				Values: []octosql.Value{
					octosql.NewString("test2"),
					octosql.NewNull(),
				},
				Parent: nil,
			},
		},
	}

	outC := NativeExecutionVariableContextToProto(c).ToNativeExecutionVariableContext()

	assert.Equal(t, c, outC)
}
