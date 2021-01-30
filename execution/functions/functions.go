package functions

import "github.com/cube2222/octosql"

func MultiplyIntInt(values []octosql.Value) (octosql.Value, error) {
	return octosql.Value{
		Type: octosql.Type{TypeID: octosql.TypeIDInt},
		Int:  values[0].Int * values[1].Int,
	}, nil
}
