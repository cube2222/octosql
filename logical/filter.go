package logical

import (
	"context"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type Filter struct {
	predicate Expression
	source    Node
}

func NewFilter(predicate Expression, child Node) *Filter {
	return &Filter{predicate: predicate, source: child}
}

func (node *Filter) Typecheck(ctx context.Context, env physical.Environment, state physical.State) physical.Node {
	source := node.source.Typecheck(ctx, env, state)
	predicate := TypecheckExpression(
		ctx,
		env.WithRecordSchema(source.Schema),
		state,
		octosql.Boolean,
		node.predicate,
	)

	return physical.Node{
		Schema:   source.Schema,
		NodeType: physical.NodeTypeFilter,
		Filter: &physical.Filter{
			Source:    source,
			Predicate: predicate,
		},
	}
}
