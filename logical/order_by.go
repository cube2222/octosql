package logical

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/physical"
)

type OrderDirection string

type OrderBy struct {
	keyExprs   []Expression
	directions []OrderDirection
	source     Node
}

func NewOrderBy(keyExprs []Expression, directions []OrderDirection, source Node) *OrderBy {
	return &OrderBy{
		keyExprs:   keyExprs,
		directions: directions,
		source:     source,
	}
}

func (node *OrderBy) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Node {
	source := node.source.Typecheck(ctx, env, logicalEnv)

	keyExprs := make([]physical.Expression, len(node.keyExprs))
	for i := range node.keyExprs {
		keyExprs[i] = node.keyExprs[i].Typecheck(ctx, env.WithRecordSchema(source.Schema), logicalEnv)
	}

	directionMultipliers := make([]int, len(node.keyExprs))
	for i, direction := range node.directions {
		switch direction {
		case "asc":
			directionMultipliers[i] = 1
		case "desc":
			directionMultipliers[i] = -1
		default:
			panic(fmt.Errorf("invalid order by direction: %s", direction))
		}
	}

	return physical.Node{
		Schema:   source.Schema,
		NodeType: physical.NodeTypeOrderBy,
		OrderBy: &physical.OrderBy{
			Source:               source,
			Key:                  keyExprs,
			DirectionMultipliers: directionMultipliers,
		},
	}
}
