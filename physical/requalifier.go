package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type Requalifier struct {
	qualifier string
	source    Node
}

func NewRequalifier(qualifier string, child Node) *Requalifier {
	return &Requalifier{qualifier: qualifier, source: child}
}

func (node *Requalifier) Materialize(ctx context.Context) (execution.Node, error) {
	materialized, err := node.source.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize source node")
	}
	return execution.NewRequalifier(node.qualifier, materialized), nil
}
