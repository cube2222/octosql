package logical

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/octosql"
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

type OrderSensitiveTransform struct {
	source            Node
	orderByKeyExprs   []Expression
	orderByDirections []OrderDirection
	limit             *Expression
}

func NewOrderSensitiveTransform(keyExprs []Expression, directions []OrderDirection, limit *Expression, source Node) *OrderSensitiveTransform {
	return &OrderSensitiveTransform{
		orderByKeyExprs:   keyExprs,
		orderByDirections: directions,
		limit:             limit,
		source:            source,
	}
}

func (node *OrderSensitiveTransform) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) (physical.Node, map[string]string) {
	source, mapping := node.source.Typecheck(ctx, env, logicalEnv)

	orderByKeyExprs := make([]physical.Expression, len(node.orderByKeyExprs))
	for i := range node.orderByKeyExprs {
		orderByKeyExprs[i] = node.orderByKeyExprs[i].Typecheck(ctx, env.WithRecordSchema(source.Schema), logicalEnv.WithRecordUniqueVariableNames(mapping))
	}

	orderByDirectionMultipliers := DirectionsToMultipliers(node.orderByDirections)

	var limit *physical.Expression
	if node.limit != nil {
		expr := TypecheckExpression(ctx, env, logicalEnv, octosql.Int, *node.limit)
		limit = &expr
	}

	return physical.Node{
		Schema:   physical.NewSchema(source.Schema.Fields, source.Schema.TimeField, physical.WithNoRetractions(true)),
		NodeType: physical.NodeTypeOrderSensitiveTransform,
		OrderSensitiveTransform: &physical.OrderSensitiveTransform{
			Source:                      source,
			OrderByKey:                  orderByKeyExprs,
			OrderByDirectionMultipliers: orderByDirectionMultipliers,
			Limit:                       limit,
		},
	}, mapping
}
