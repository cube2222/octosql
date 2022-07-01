package logical

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/optimizer"
	"github.com/cube2222/octosql/physical"
)

type StreamJoin struct {
	left, right Node
}

func NewStreamJoin(left, right Node) *StreamJoin {
	return &StreamJoin{
		left:  left,
		right: right,
	}
}

func (node *StreamJoin) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) (physical.Node, map[string]string) {
	left, leftMapping := node.left.Typecheck(ctx, env, logicalEnv)
	right, rightMapping := node.right.Typecheck(ctx, env, logicalEnv)

	for k, v := range leftMapping {
		// Put all mapped variables into one map.
		// Left mapping takes precedence. Duplicates get overwritten.
		// TODO: Duplicates should be handled well.
		rightMapping[k] = v
	}

	return physical.Node{
		Schema: physical.Schema{
			Fields:        append(left.Schema.Fields[:len(left.Schema.Fields):len(left.Schema.Fields)], right.Schema.Fields[:len(right.Schema.Fields):len(right.Schema.Fields)]...),
			TimeField:     left.Schema.TimeField,
			NoRetractions: left.Schema.NoRetractions && right.Schema.NoRetractions,
		},
		NodeType: physical.NodeTypeStreamJoin,
		StreamJoin: &physical.StreamJoin{
			Left:  left,
			Right: right,
		},
	}, rightMapping
}

type OuterJoin struct {
	left, right     Node
	predicate       Expression
	isLeft, isRight bool
}

func NewOuterJoin(left, right Node, predicate Expression, isLeft, isRight bool) *OuterJoin {
	return &OuterJoin{
		left:      left,
		right:     right,
		predicate: predicate,
		isLeft:    isLeft,
		isRight:   isRight,
	}
}

func (node *OuterJoin) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) (physical.Node, map[string]string) {
	left, leftMapping := node.left.Typecheck(ctx, env, logicalEnv)
	right, rightMapping := node.right.Typecheck(ctx, env, logicalEnv)

	outMapping := rightMapping

	for k, v := range leftMapping {
		// Put all mapped variables into one map.
		// Left mapping takes precedence. Duplicates get overwritten.
		// TODO: Duplicates should be handled well.
		outMapping[k] = v
	}

	predicate := node.predicate.Typecheck(ctx, env.WithRecordSchema(right.Schema).WithRecordSchema(left.Schema), logicalEnv.WithRecordUniqueVariableNames(outMapping))

	filterPredicates := predicate.SplitByAnd()
	var leftKey, rightKey []physical.Expression

	for i := range filterPredicates {
		if filterPredicates[i].ExpressionType != physical.ExpressionTypeFunctionCall || filterPredicates[i].FunctionCall.Name != "=" {
			panic(fmt.Errorf("outer join predicate must be a conjunction of equalities"))
		}
		firstPart := filterPredicates[i].FunctionCall.Arguments[0]
		secondPart := filterPredicates[i].FunctionCall.Arguments[1]
		firstPartVariables := firstPart.VariablesUsed()
		firstPartUsesLeftVariables := optimizer.UsesVariablesFromSchema(left.Schema, firstPartVariables)
		firstPartUsesRightVariables := optimizer.UsesVariablesFromSchema(right.Schema, firstPartVariables)
		secondPartVariables := secondPart.VariablesUsed()
		secondPartUsesLeftVariables := optimizer.UsesVariablesFromSchema(left.Schema, secondPartVariables)
		secondPartUsesRightVariables := optimizer.UsesVariablesFromSchema(right.Schema, secondPartVariables)

		if firstPartUsesLeftVariables && !firstPartUsesRightVariables &&
			!secondPartUsesLeftVariables && secondPartUsesRightVariables {
			leftKey = append(leftKey, firstPart)
			rightKey = append(rightKey, secondPart)
		} else if !firstPartUsesLeftVariables && firstPartUsesRightVariables &&
			secondPartUsesLeftVariables && !secondPartUsesRightVariables {
			rightKey = append(rightKey, firstPart)
			leftKey = append(leftKey, secondPart)
		} else {
			panic(fmt.Errorf("outer join predicate equality predicate left and right side must each reference only one of the input tables"))
		}
	}

	outSchemaFields := append(left.Schema.Fields[:len(left.Schema.Fields):len(left.Schema.Fields)], right.Schema.Fields[:len(right.Schema.Fields):len(right.Schema.Fields)]...)
	if node.isLeft {
		for i := len(left.Schema.Fields); i < len(outSchemaFields); i++ {
			outSchemaFields[i] = physical.SchemaField{
				Name: outSchemaFields[i].Name,
				Type: octosql.TypeSum(outSchemaFields[i].Type, octosql.Null),
			}
		}
	}
	if node.isRight {
		for i := 0; i < len(left.Schema.Fields); i++ {
			outSchemaFields[i] = physical.SchemaField{
				Name: outSchemaFields[i].Name,
				Type: octosql.TypeSum(outSchemaFields[i].Type, octosql.Null),
			}
		}
	}

	return physical.Node{
		Schema: physical.Schema{
			Fields:        outSchemaFields,
			TimeField:     left.Schema.TimeField,
			NoRetractions: left.Schema.NoRetractions && right.Schema.NoRetractions,
		},
		NodeType: physical.NodeTypeOuterJoin,
		OuterJoin: &physical.OuterJoin{
			Left:     left,
			Right:    right,
			LeftKey:  leftKey,
			RightKey: rightKey,
			IsLeft:   node.isLeft,
			IsRight:  node.isRight,
		},
	}, outMapping
}

type LookupJoin struct {
	left, right Node
}

func NewLookupJoin(left, right Node) *LookupJoin {
	return &LookupJoin{
		left:  left,
		right: right,
	}
}

func (node *LookupJoin) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) (physical.Node, map[string]string) {
	// TODO: This should currently fail when the joined stream has a time field / is endless.
	left, leftMapping := node.left.Typecheck(ctx, env, logicalEnv)
	right, rightMapping := node.right.Typecheck(ctx, env.WithRecordSchema(left.Schema), logicalEnv.WithRecordUniqueVariableNames(leftMapping))

	for k, v := range leftMapping {
		// Put all mapped variables into one map.
		// Left mapping takes precedence. Duplicates get overwritten.
		rightMapping[k] = v
	}

	return physical.Node{
		Schema: physical.Schema{
			Fields:    append(left.Schema.Fields[:len(left.Schema.Fields):len(left.Schema.Fields)], right.Schema.Fields[:len(right.Schema.Fields):len(right.Schema.Fields)]...),
			TimeField: left.Schema.TimeField,
		},
		NodeType: physical.NodeTypeLookupJoin,
		LookupJoin: &physical.LookupJoin{
			Source: left,
			Joined: right,
		},
	}, rightMapping
}
