package logical

import (
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type FunctionExpression struct {
	Name  string
	Child Expression
}

func NewFunctionExpression(name string, child Expression) *FunctionExpression {
	return &FunctionExpression{
		Name:  name,
		Child: child,
	}
}

func (fe *FunctionExpression) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, octosql.Variables, error) {
	child, variables, err := fe.Child.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Couldn't get child expression")
	}

	return physical.NewFunctionExpression(fe.Name, child), variables, nil
}
