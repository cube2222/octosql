package logical

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/physical"
)

type OrderDirection string

func DirectionsToMultipliers(directions []OrderDirection) []int {
	directionMultipliers := make([]int, len(directions))
	for i, direction := range directions {
		switch direction {
		case "asc":
			directionMultipliers[i] = 1
		case "desc":
			directionMultipliers[i] = -1
		default:
			panic(fmt.Errorf("invalid order by direction: %s", direction))
		}
	}

	return directionMultipliers
}

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

func (node *OrderBy) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) (physical.Node, map[string]string) {
	source, mapping := node.source.Typecheck(ctx, env, logicalEnv)

	keyExprs := make([]physical.Expression, len(node.keyExprs))
	for i := range node.keyExprs {
		keyExprs[i] = node.keyExprs[i].Typecheck(ctx, env.WithRecordSchema(source.Schema), logicalEnv.WithRecordUniqueVariableNames(mapping))
	}

	directionMultipliers := DirectionsToMultipliers(node.directions)

	return physical.Node{
		Schema:   source.Schema,
		NodeType: physical.NodeTypeOrderBy,
		OrderBy: &physical.OrderBy{
			Source:               source,
			Key:                  keyExprs,
			DirectionMultipliers: directionMultipliers,
		},
	}, mapping
}
