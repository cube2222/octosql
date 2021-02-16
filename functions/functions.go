package functions

import (
	"time"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

// TODO: Change this to the final map in place.
func FunctionMap() map[string][]physical.FunctionDescriptor {
	return map[string][]physical.FunctionDescriptor{
		"<": {
			// TODO: Specializations for concrete primitive types.
			{
				ArgumentTypes: []octosql.Type{octosql.Any, octosql.Any},
				OutputType:    octosql.Boolean,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					// TODO: Null should probably not equal Null.
					return octosql.NewBoolean(values[0].Compare(values[1]) < 0), nil
				},
			},
		},
		"<=": {
			// TODO: Specializations for concrete primitive types.
			{
				ArgumentTypes: []octosql.Type{octosql.Any, octosql.Any},
				OutputType:    octosql.Boolean,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					// TODO: Null should probably not equal Null.
					return octosql.NewBoolean(values[0].Compare(values[1]) <= 0), nil
				},
			},
		},
		"=": {
			// TODO: Specializations for concrete primitive types.
			{
				ArgumentTypes: []octosql.Type{octosql.Any, octosql.Any},
				OutputType:    octosql.Boolean,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					// TODO: Null should probably not equal Null.
					return octosql.NewBoolean(values[0].Compare(values[1]) == 0), nil
				},
			},
		},
		">=": {
			// TODO: Specializations for concrete primitive types.
			{
				ArgumentTypes: []octosql.Type{octosql.Any, octosql.Any},
				OutputType:    octosql.Boolean,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					// TODO: Null should probably not equal Null.
					return octosql.NewBoolean(values[0].Compare(values[1]) >= 0), nil
				},
			},
		},
		">": {
			// TODO: Specializations for concrete primitive types.
			{
				ArgumentTypes: []octosql.Type{octosql.Any, octosql.Any},
				OutputType:    octosql.Boolean,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					// TODO: Null should probably not equal Null.
					return octosql.NewBoolean(values[0].Compare(values[1]) > 0), nil
				},
			},
		},
		"int": {
			{
				ArgumentTypes: []octosql.Type{octosql.Float},
				OutputType:    octosql.Int,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewInt(int(values[0].Float)), nil
				},
			},
		},
		"now": {
			{
				ArgumentTypes: []octosql.Type{},
				OutputType:    octosql.Time,
				Function: func(values []octosql.Value) (octosql.Value, error) {
					return octosql.NewTime(time.Now()), nil
				},
			},
		},
		"time_from_unix": {
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
		},
	}
}
