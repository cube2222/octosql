package logical

import (
	"context"

	"github.com/cube2222/octosql/physical"
)

type JoinType string

const (
	JoinTypeLeft  JoinType = "Left"
	JoinTypeInner JoinType = "Inner"
)

type JoinStrategy string

const (
	JoinStrategyUndefined JoinStrategy = "UNDEFINED"
	JoinStrategyLookup    JoinStrategy = "LOOKUP"
	JoinStrategyStream    JoinStrategy = "STREAM"
)

// TODO: Support left joins.

type Join struct {
	left, right Node
}

func NewJoin(left, right Node) *Join {
	return &Join{
		left:  left,
		right: right,
	}
}

func (node *Join) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Node {
	left := node.left.Typecheck(ctx, env, logicalEnv)
	right := node.right.Typecheck(ctx, env, logicalEnv)

	return physical.Node{
		Schema: physical.Schema{
			Fields:    append(left.Schema.Fields[:len(left.Schema.Fields):len(left.Schema.Fields)], right.Schema.Fields[:len(right.Schema.Fields):len(right.Schema.Fields)]...),
			TimeField: left.Schema.TimeField,
		},
		NodeType: physical.NodeTypeStreamJoin,
		StreamJoin: &physical.StreamJoin{
			Left:  left,
			Right: right,
		},
	}
}

// func usesVariablesFromLeftOrRight(left, right physical.Schema, variables []string) (usesLeft bool, usesRight bool) {
// 	for _, name := range variables {
// 		var matchedLeft, matchedRight bool
// 		for _, field := range left.Fields {
// 			if physical.VariableNameMatchesField(name, field.Name) {
// 				usesLeft = true
// 				matchedLeft = true
// 				break
// 			}
// 		}
// 		for _, field := range right.Fields {
// 			if physical.VariableNameMatchesField(name, field.Name) {
// 				usesRight = true
// 				matchedRight = true
// 				break
// 			}
// 		}
// 		if matchedLeft && matchedRight {
// 			panic(fmt.Errorf("ambiguous variable Name in join predicate: %s", name))
// 		}
// 	}
// 	return
// }

type LateralJoin struct {
	left, right Node
}

func NewLateralJoin(left, right Node) *LateralJoin {
	return &LateralJoin{
		left:  left,
		right: right,
	}
}

func (node *LateralJoin) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Node {
	panic("implement me")
}

// func (node *Join) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Node {
// 	if node.strategy == JoinStrategyUndefined || node.strategy == JoinStrategyStream {
// 		left := node.left.Typecheck(ctx, env, logicalEnv)
// 		right := node.right.Typecheck(ctx, env, logicalEnv)
// 		predicate := node.predicate.Typecheck(ctx, env.WithRecordSchema(left.Schema).WithRecordSchema(right.Schema), logicalEnv)
//
// 		parts := predicate.SplitByAnd()
// 		leftKey := make([]physical.Expression, len(parts))
// 		rightKey := make([]physical.Expression, len(parts))
// 		var streamJoinNotOkErr error
// 		for i, part := range parts {
// 			if part.ExpressionType != physical.ExpressionTypeFunctionCall {
// 				panic("only equality joins currently supported")
// 			}
// 			if part.FunctionCall.Name != "=" {
// 				panic("only equality joins currently supported")
// 			}
// 			firstPart := part.FunctionCall.Arguments[0]
// 			secondPart := part.FunctionCall.Arguments[1]
// 			// TODO: Move ambiguity errors to typecheck phase.
// 			firstPartVariables := firstPart.VariablesUsed()
// 			firstPartUsesLeft, firstPartUsesRight := usesVariablesFromLeftOrRight(left.Schema, right.Schema, firstPartVariables)
// 			secondPartVariables := secondPart.VariablesUsed()
// 			secondPartUsesLeft, secondPartUsesRight := usesVariablesFromLeftOrRight(left.Schema, right.Schema, secondPartVariables)
//
// 			if firstPartUsesLeft && firstPartUsesRight {
// 				streamJoinNotOkErr = fmt.Errorf("left hand side of %d join predicate equality uses variables from both input tables", i)
// 				break
// 			}
// 			if secondPartUsesLeft && secondPartUsesRight {
// 				streamJoinNotOkErr = fmt.Errorf("right hand side of %d join predicate equality uses variables from both input tables", i)
// 				break
// 			}
//
// 			if firstPartUsesLeft && secondPartUsesLeft {
// 				streamJoinNotOkErr = fmt.Errorf("both side of %d join predicate equality use variables from the left input table", i)
// 				break
// 			}
// 			if firstPartUsesRight && secondPartUsesRight {
// 				streamJoinNotOkErr = fmt.Errorf("both side of %d join predicate equality use variables from the right input table", i)
// 				break
// 			}
//
// 			if !firstPartUsesRight && !secondPartUsesLeft {
// 				leftKey[i] = firstPart
// 				rightKey[i] = secondPart
// 			} else if !firstPartUsesLeft && !secondPartUsesRight {
// 				leftKey[i] = secondPart
// 				rightKey[i] = firstPart
// 			}
// 		}
// 		if streamJoinNotOkErr == nil {
// 			return physical.Node{
// 				Schema: physical.Schema{
// 					Fields:    append(left.Schema.Fields[:len(left.Schema.Fields):len(left.Schema.Fields)], right.Schema.Fields[:len(right.Schema.Fields):len(right.Schema.Fields)]...),
// 					TimeField: left.Schema.TimeField,
// 				},
// 				NodeType: physical.NodeTypeStreamJoin,
// 				StreamJoin: &physical.StreamJoin{
// 					Left:     left,
// 					Right:    right,
// 					LeftKey:  leftKey,
// 					RightKey: rightKey,
// 				},
// 			}
// 		}
// 		if node.strategy == JoinStrategyStream {
// 			panic(streamJoinNotOkErr)
// 		}
// 	}
//
// 	source := node.left.Typecheck(ctx, env, logicalEnv)
// 	joined := NewFilter(node.predicate, node.right).Typecheck(ctx, env.WithRecordSchema(source.Schema), logicalEnv)
//
// 	return physical.Node{
// 		Schema: physical.Schema{
// 			Fields:    append(source.Schema.Fields[:len(source.Schema.Fields):len(source.Schema.Fields)], joined.Schema.Fields[:len(joined.Schema.Fields):len(joined.Schema.Fields)]...),
// 			TimeField: source.Schema.TimeField,
// 		},
// 		NodeType: physical.NodeTypeLookupJoin,
// 		LookupJoin: &physical.LookupJoin{
// 			Source: source,
// 			Joined: joined,
// 		},
// 	}
// }
