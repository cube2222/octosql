package logical

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type Map struct {
	expressions []NamedExpression
	source      Node
	keep        bool
}

func (mapNode *Map) Visualize() *graph.Node {
	n := graph.NewNode("Map")
	n.AddField("keep", fmt.Sprint(mapNode.keep))

	if mapNode.source != nil {
		n.AddChild("source", mapNode.source.Visualize())
	}
	if len(mapNode.expressions) != 0 {
		for idx, expr := range mapNode.expressions {
			n.AddChild("expr_"+strconv.Itoa(idx), expr.Visualize())
		}
	}
	return n
}

func NewMap(expressions []NamedExpression, child Node, keep bool) *Map {
	return &Map{expressions: expressions, source: child, keep: keep}
}

func (node *Map) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, octosql.Variables, error) {
	physicalExprs := make([]physical.NamedExpression, len(node.expressions))
	variables := octosql.NoVariables()
	for i := range node.expressions {
		physicalExpr, exprVariables, err := node.expressions[i].PhysicalNamed(ctx, physicalCreator)
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

	return physical.NewMap(physicalExprs, child, node.keep), variables, nil
}
