package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type Distinct struct {
	child Node
}

func NewDistinct(child Node) *Distinct {
	return &Distinct{child: child}
}

func (node *Distinct) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) ([]physical.Node, octosql.Variables, error) {
	typed, ok := node.child.(*Map)
	if !ok {
		return nil, nil, errors.New("expected a map as distinct's child")
	}

	expressionCount := len(typed.expressions)

	aggregates := make([]Aggregate, 0)
	names := make([]octosql.VariableName, 0)
	as := make([]octosql.VariableName, expressionCount)
	castExpressions := make([]Expression, expressionCount)

	for i, expr := range typed.expressions {
		if _, ok := expr.(*StarExpression); !ok {
			aggregates = append(aggregates, First)
			names = append(names, expr.Name())
		}

		castExpressions[i] = expr.(Expression)
	}

	groupByNode := NewGroupBy(typed, castExpressions, names, aggregates, as, nil)

	return groupByNode.Physical(ctx, physicalCreator)

	//sourceNodes, variables, err := node.child.Physical(ctx, physicalCreator)
	//if err != nil {
	//	return nil, nil, errors.Wrap(err, "couldn't get source nodes physical plan in distinct")
	//}
	//
	//outNodes := physical.NewShuffle(1, sourceNodes, physical.DefaultShuffleStrategy)
	//for i := range outNodes {
	//	outNodes[i] = physical.NewDistinct(outNodes[i])
	//}
	//
	//return outNodes, variables, nil
}
