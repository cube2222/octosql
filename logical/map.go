package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type Map struct {
	expressions []Expression
	source      Node
}

func NewMap(expressions []Expression, child Node) *Map {
	return &Map{expressions: expressions, source: child}
}

func (node *Map) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, octosql.Variables, error) {
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

		physicalExprs[i] = physicalExpr
	}

	child, childVariables, err := node.source.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for map source node")
	}

	variables, err = childVariables.MergeWith(variables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't merge variables for map source")
	}

	return physical.NewMap(physicalExprs, child), variables, nil
}
