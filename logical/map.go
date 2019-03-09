package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type Map struct {
	expressions []Expression
	child       Node
}

func NewMap(expressions []Expression, child Node) *Map {
	return &Map{expressions: expressions, child: child}
}

func (node *Map) Physical(ctx context.Context, physicalCreator PhysicalPlanCreator) (physical.Node, octosql.Variables, error) {
	physicalExprs := make([]physical.Expression, len(node.expressions))
	variables := octosql.NoVariables()
	for i := range node.expressions {
		physicalExpr, exprVariables, err := node.expressions[i].Physical(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrapf(
				err,
				"couldn't get physical plan for map expression with index %d", i,
			)
		}
		variables, err = variables.MergeWith(exprVariables)
		if err != nil {
			return nil, nil, errors.Wrapf(
				err,
				"couldn't merge variables with those of map expression with index %d", i,
			)
		}

		physicalExprs = append(physicalExprs, physicalExpr)
	}

	child, childVariables, err := node.child.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for map child node")
	}

	variables, err = childVariables.MergeWith(variables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't merge variables for map child")
	}

	return physical.NewMap(physicalExprs, child), variables, nil
}
