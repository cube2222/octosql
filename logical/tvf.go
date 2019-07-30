package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type TableValuedFunction struct {
	name      string
	arguments map[octosql.VariableName]Expression
}

func NewTableValuedFunction(name string, arguments map[octosql.VariableName]Expression) *TableValuedFunction {
	return &TableValuedFunction{name: name, arguments: arguments}
}

func (node *TableValuedFunction) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, octosql.Variables, error) {
	variables := octosql.NoVariables()

	physArguments := make(map[octosql.VariableName]physical.Expression)
	for k, v := range node.arguments {
		physArg, argVariables, err := v.Physical(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrapf(
				err,
				"couldn't get physical plan for table valued function argument \"%s\"", k,
			)
		}

		variables, err = variables.MergeWith(argVariables)
		if err != nil {
			return nil, nil, errors.Wrapf(
				err,
				"couldn't merge variables with those of table valued function argument \"%s\"", k,
			)
		}

		physArguments[k] = physArg
	}

	return physical.NewTableValuedFunction(
		node.name,
		physArguments,
	), variables, nil
}
