package logical

import (
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type FunctionExpression struct {
	name      string
	arguments []Expression
}

func NewFunctionExpression(name string, args []Expression) *FunctionExpression {
	return &FunctionExpression{
		name:      name,
		arguments: args,
	}
}

func (fe *FunctionExpression) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, octosql.Variables, error) {
	args := make([]physical.Expression, 0)
	variables := octosql.NoVariables()

	for i := range fe.arguments {
		arg := fe.arguments[i]

		phys, vars, err := arg.Physical(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't get physical expression from argument")
		}

		variables, err = variables.MergeWith(vars)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't merge variables")
		}

		args = append(args, phys)
	}

	return physical.NewFunctionExpression(fe.name, args), variables, nil
}
