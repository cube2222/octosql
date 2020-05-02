package logical

import (
	"context"
	"runtime"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/physical"
)

type Distinct struct {
	child Node
}

func (node *Distinct) Visualize() *graph.Node {
	n := graph.NewNode("Distinct")
	if node.child != nil {
		n.AddChild("source", node.child.Visualize())
	}
	return n
}

func NewDistinct(child Node) *Distinct {
	return &Distinct{child: child}
}

func (node *Distinct) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) ([]physical.Node, octosql.Variables, error) {
	groupByParallelism, err := config.GetInt(
		physicalCreator.physicalConfig,
		"groupByParallelism",
		config.WithDefault(runtime.GOMAXPROCS(0)),
	)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get groupByParallelism configuration")
	}

	sourceNodes, variables, err := node.child.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source nodes physical plan in distinct")
	}

	outNodes := physical.NewShuffle(
		groupByParallelism,
		physical.NewKeyHashingStrategy([]physical.Expression{physical.NewRecordExpression()}),
		sourceNodes,
	)
	for i := range outNodes {
		outNodes[i] = physical.NewDistinct(outNodes[i])
	}

	return outNodes, variables, nil
}
