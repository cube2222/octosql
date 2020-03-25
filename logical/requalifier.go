package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type Requalifier struct {
	qualifier string
	source    Node
}

func (requalifier Requalifier) Visualize() *graph.Node {
	n := graph.NewNode("Requalifier")
	n.AddField("qualifier", requalifier.qualifier)
	if requalifier.source != nil {
		n.AddChild("source", requalifier.source.Visualize())
	}
	return n
}

func NewRequalifier(qualifier string, child Node) *Requalifier {
	return &Requalifier{qualifier: qualifier, source: child}
}

func (node *Requalifier) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, octosql.Variables, error) {
	child, variables, err := node.source.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for requalifier node")
	}

	return physical.NewRequalifier(node.qualifier, child), variables, nil
}
