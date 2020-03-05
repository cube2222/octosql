package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type Requalifier struct {
	qualifier string
	source    Node
}

func NewRequalifier(qualifier string, child Node) *Requalifier {
	return &Requalifier{qualifier: qualifier, source: child}
}

func (node *Requalifier) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) ([]physical.Node, octosql.Variables, error) {
	sourceNodes, variables, err := node.source.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for requalifier source nodes")
	}

	outputNodes := make([]physical.Node, len(sourceNodes))
	for i := range outputNodes {
		outputNodes[i] = physical.NewRequalifier(node.qualifier, sourceNodes[i])
	}

	return outputNodes, variables, nil
}
