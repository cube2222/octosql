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

func (node *Filter) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) (physical.Node, map[string]string) {
	source, mapping := node.source.Typecheck(ctx, env, logicalEnv)
	predicate := TypecheckExpression(
		ctx,
		env.WithRecordSchema(source.Schema),
		logicalEnv.WithRecordUniqueVariableNames(mapping),
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
	}, mapping
}
