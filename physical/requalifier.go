package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
)

type Requalifier struct {
	qualifier string
	source    Node
}

func NewRequalifier(qualifier string, child Node) *Requalifier {
	return &Requalifier{qualifier: qualifier, source: child}
}

func (node *Requalifier) Materialize(ctx context.Context) execution.Node {
	return execution.NewRequalifier(node.qualifier, node.source.Materialize(ctx))
}
