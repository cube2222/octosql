package physical

import (
	"context"
	"github.com/cube2222/octosql"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical/metadata"
	"github.com/pkg/errors"
)

type OrderDirection string

const (
	Ascending  OrderDirection = "asc"
	Descending OrderDirection = "desc"
)

type OrderBy struct {
	Expressions []Expression
	Directions  []OrderDirection
	Source      Node
}

func NewOrderBy(expressions []Expression, directions []OrderDirection, source Node) *OrderBy {
	return &OrderBy{
		Expressions: expressions,
		Directions:  directions,
		Source:      source,
	}
}

func (node *OrderBy) Transform(ctx context.Context, transformers *Transformers) Node {
	exprs := make([]Expression, len(node.Expressions))
	for i := range node.Expressions {
		exprs[i] = node.Expressions[i].Transform(ctx, transformers)
	}

	var transformed Node = &OrderBy{
		Expressions: exprs,
		Directions:  node.Directions,
		Source:      node.Source.Transform(ctx, transformers),
	}

	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *OrderBy) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	exprs := make([]execution.Expression, len(node.Expressions))
	for i := range node.Expressions {
		var err error
		exprs[i], err = node.Expressions[i].Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize expression with index %v", i)
		}
	}

	triggerPrototype := execution.NewWatermarkTrigger()

	directions := make([]execution.OrderDirection, len(node.Expressions))
	for i := range node.Directions {
		directions[i] = execution.OrderDirection(node.Directions[i])
	}

	sourceNode, err := node.Source.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get execution node from order by source")
	}

	eventTimeField := node.orderByEventTime(node.Source.Metadata())

	return execution.NewOrderBy(matCtx.Storage, sourceNode, exprs, directions, eventTimeField, triggerPrototype), nil
}

func (node *OrderBy) orderByEventTime(sourceMetadata *metadata.NodeMetadata) octosql.VariableName {
	if !sourceMetadata.EventTimeField().Empty() {
		if node.Directions[0] == Ascending && node.Expressions[0].(*Variable).ExpressionName() == sourceMetadata.EventTimeField() {
			return sourceMetadata.EventTimeField()
		}
	}

	return octosql.NewVariableName("")
}

func (node *OrderBy) Metadata() *metadata.NodeMetadata {
	return metadata.NewNodeMetadataFromMetadata(node.Source.Metadata())
}

func (node *OrderBy) Visualize() *graph.Node {
	n := graph.NewNode("Order By")
	n.AddChild("source", node.Source.Visualize())

	for i := range n.Children {
		n.AddChild(string(node.Directions[i]), node.Expressions[i].Visualize())
	}

	return n
}
