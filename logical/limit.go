package logical

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type Limit struct {
	source Node
	limit  Expression
}

func NewLimit(limit Expression, source Node) *Limit {
	return &Limit{
		limit:  limit,
		source: source,
	}
}

func (node *Limit) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) (physical.Node, map[string]string) {
	source, mapping := node.source.Typecheck(ctx, env, logicalEnv)
	if !source.Schema.NoRetractions {
		panic(fmt.Sprintf("LIMIT must either be in the top-level statement or operate on a stream on which retractions are not possible"))
	}

	limit := TypecheckExpression(ctx, env, logicalEnv, octosql.Int, node.limit)

	return physical.Node{
		Schema:   source.Schema,
		NodeType: physical.NodeTypeLimit,
		Limit: &physical.Limit{
			Source: source,
			Limit:  limit,
		},
	}, mapping
}
