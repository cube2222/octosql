package functions

import (
	"time"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

// TODO: Change this to the final map in place.
var Equals = []physical.FunctionDescriptor{
	// TODO: Specializations for concrete primitive types.
	{
		ArgumentTypes: []octosql.Type{octosql.Any, octosql.Any},
		OutputType:    octosql.Boolean,
		Function: func(values []octosql.Value) (octosql.Value, error) {
			// TODO: Null should probably not equal Null.
			return octosql.NewBoolean(values[0].Compare(values[1]) == 0), nil
		},
	},
}
var TimeFromUnix = []physical.FunctionDescriptor{
	{
		ArgumentTypes: []octosql.Type{octosql.Int},
		OutputType:    octosql.Time,
		Function: func(values []octosql.Value) (octosql.Value, error) {
			return octosql.NewTime(time.Unix(int64(values[0].Int), 0)), nil
		},
	},
	{
		ArgumentTypes: []octosql.Type{octosql.Float},
		OutputType:    octosql.Time,
		Function: func(values []octosql.Value) (octosql.Value, error) {
			return octosql.NewTime(time.Unix(int64(values[0].Float), 0)), nil
		},
	},
}
var Int = []physical.FunctionDescriptor{
	{
		ArgumentTypes: []octosql.Type{octosql.Float},
		OutputType:    octosql.Int,
		Function: func(values []octosql.Value) (octosql.Value, error) {
			return octosql.NewInt(int(values[0].Float)), nil
		},
	},
}
