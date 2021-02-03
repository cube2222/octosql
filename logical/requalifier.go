package logical

import (
	"context"

	"github.com/cube2222/octosql/physical"
)

type Requalifier struct {
	qualifier string
	source    Node
}

func NewRequalifier(qualifier string, child Node) *Requalifier {
	return &Requalifier{qualifier: qualifier, source: child}
}

func (node *Requalifier) Typecheck(ctx context.Context, env physical.Environment, state physical.State) ([]physical.Node, error) {
	panic("implement me")
}
