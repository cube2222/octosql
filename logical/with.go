package logical

import (
	"context"

	"github.com/cube2222/octosql/physical"
)

type With struct {
	cteNames []string
	cteNodes []Node
	source   Node
}

func NewWith(cteNames []string, cteNodes []Node, source Node) *With {
	return &With{
		cteNames: cteNames,
		cteNodes: cteNodes,
		source:   source,
	}
}

func (node *With) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Node {
	newCTEs := make(map[string]physical.Node)
	for k, v := range logicalEnv.CommonTableExpressions {
		newCTEs[k] = v
	}

	for i := range node.cteNodes {
		newCTEs[node.cteNames[i]] = node.cteNodes[i].Typecheck(ctx, env, Environment{CommonTableExpressions: newCTEs})
	}

	return node.source.Typecheck(ctx, env, Environment{CommonTableExpressions: newCTEs})
}
