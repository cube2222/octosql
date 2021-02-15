package physical

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/nodes"
	"github.com/cube2222/octosql/octosql"
)

type Node struct {
	Schema Schema

	NodeType NodeType
	// Only one of the below may be non-null.
	Datasource          *Datasource
	Filter              *Filter
	GroupBy             *GroupBy
	Join                *Join
	Map                 *Map
	OrderBy             *OrderBy
	Requalifier         *Requalifier
	TableValuedFunction *TableValuedFunction
}

type Schema struct {
	Fields []SchemaField
	// TimeField is -1 if not present.
	TimeField int
}

func NewSchema(fields []SchemaField, timeField int) Schema {
	return Schema{
		Fields:    fields,
		TimeField: timeField,
	}
}

type SchemaField struct {
	Name string
	Type octosql.Type
}

type NodeType int

const (
	NodeTypeDatasource NodeType = iota
	NodeTypeFilter
	NodeTypeGroupBy
	NodeTypeJoin
	NodeTypeMap
	NodeTypeOrderBy
	NodeTypeRequalifier
	NodeTypeTableValuedFunction
)

type Datasource struct {
	Name                     string
	DatasourceImplementation DatasourceImplementation
	Predicates               []Expression
}

type Filter struct {
	Source    Node
	Predicate Expression
}

type GroupBy struct {
	Source               Node
	Aggregates           []Aggregate
	AggregateExpressions []Expression
	Key                  []Expression
	Trigger              Trigger
}

type Aggregate struct {
	Name                string
	AggregateDescriptor AggregateDescriptor
}

type Join struct {
	Left, Right Node
	On          Expression
}

type Map struct {
	Source      Node
	Expressions []Expression
	Aliases     []*string
}

type OrderBy struct {
	Source               Node
	Key                  []Expression
	DirectionMultipliers []int
}

type Requalifier struct {
	Source    Node
	Qualifier string
}

type TableValuedFunction struct {
	Name               string
	Arguments          map[string]TableValuedFunctionArgument
	FunctionDescriptor TableValuedFunctionDescriptor
}

type TableValuedFunctionArgument struct {
	TableValuedFunctionArgumentType TableValuedFunctionArgumentType
	// Only one of the below may be non-null.
	Expression *TableValuedFunctionArgumentExpression
	Table      *TableValuedFunctionArgumentTable
	Descriptor *TableValuedFunctionArgumentDescriptor
}

func (arg *TableValuedFunctionArgument) String() string {
	switch arg.TableValuedFunctionArgumentType {
	case TableValuedFunctionArgumentTypeExpression:
		return arg.Expression.Expression.Type.String()
	case TableValuedFunctionArgumentTypeTable:
		return "TABLE"
	case TableValuedFunctionArgumentTypeDescriptor:
		return "DESCRIPTOR"
	}
	panic("unexhaustive table valued function argument type match")
}

type TableValuedFunctionArgumentType int

const (
	TableValuedFunctionArgumentTypeExpression TableValuedFunctionArgumentType = iota
	TableValuedFunctionArgumentTypeTable
	TableValuedFunctionArgumentTypeDescriptor
)

type TableValuedFunctionArgumentExpression struct {
	Expression Expression
}

type TableValuedFunctionArgumentTable struct {
	Table Node
}

type TableValuedFunctionArgumentDescriptor struct {
	Descriptor string
}

func (node *Node) Materialize(ctx context.Context, env Environment) (execution.Node, error) {
	switch node.NodeType {
	case NodeTypeDatasource:
		return node.Datasource.DatasourceImplementation.Materialize(ctx, env, node.Datasource.Predicates)
	case NodeTypeFilter:
		source, err := node.Filter.Source.Materialize(ctx, env)
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize filter source: %w", err)
		}
		predicate, err := node.Filter.Predicate.Materialize(ctx, env.WithRecordSchema(node.Filter.Source.Schema))
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize filter predicate: %w", err)
		}
		return nodes.NewFilter(source, predicate), nil
	case NodeTypeGroupBy:
		source, err := node.GroupBy.Source.Materialize(ctx, env)
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize group by source: %w", err)
		}
		key := make([]execution.Expression, len(node.GroupBy.Key))
		for i := range node.GroupBy.Key {
			expr, err := node.GroupBy.Key[i].Materialize(ctx, env.WithRecordSchema(node.GroupBy.Source.Schema))
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize group by key expression with index %d: %w", i, err)
			}
			key[i] = expr
		}
		aggregates := make([]func() nodes.Aggregate, len(node.GroupBy.Aggregates))
		for i := range node.GroupBy.Aggregates {
			aggregates[i] = node.GroupBy.Aggregates[i].AggregateDescriptor.Prototype
		}
		expressions := make([]execution.Expression, len(node.GroupBy.AggregateExpressions))
		for i := range node.GroupBy.AggregateExpressions {
			expr, err := node.GroupBy.AggregateExpressions[i].Materialize(ctx, env.WithRecordSchema(node.GroupBy.Source.Schema))
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize group by aggregate expression with index %d: %w", i, err)
			}
			expressions[i] = expr
		}
		trigger := node.GroupBy.Trigger.Materialize(ctx, env)

		return nodes.NewGroupBy(aggregates, expressions, key, source, trigger), nil
	case NodeTypeJoin:
		left, err := node.Join.Left.Materialize(ctx, env)
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize left join source: %w", err)
		}
		right, err := node.Join.Right.Materialize(ctx, env)
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize right join source: %w", err)
		}

		// TODO: Move to physical.Join
		parts := node.Join.On.SplitByAnd()
		leftKey := make([]Expression, len(parts))
		rightKey := make([]Expression, len(parts))
		for i, part := range parts {
			if part.ExpressionType != ExpressionTypeFunctionCall {
				panic("only equality joins currently supported")
			}
			if part.FunctionCall.Name != "=" {
				panic("only equality joins currently supported")
			}
			firstPart := part.FunctionCall.Arguments[0]
			secondPart := part.FunctionCall.Arguments[1]
			// TODO: Move ambiguity errors to typecheck phase.
			firstPartVariables := firstPart.VariablesUsed()
			firstPartUsesLeft, firstPartUsesRight, err := usesVariablesFromLeftOrRight(node.Join.Left.Schema, node.Join.Right.Schema, firstPartVariables)
			if err != nil {
				return nil, err
			}
			secondPartVariables := secondPart.VariablesUsed()
			secondPartUsesLeft, secondPartUsesRight, err := usesVariablesFromLeftOrRight(node.Join.Left.Schema, node.Join.Right.Schema, secondPartVariables)
			if err != nil {
				return nil, err
			}

			if firstPartUsesLeft && firstPartUsesRight {
				return nil, fmt.Errorf("left hand side of %d join predicate equality uses variables from both input tables", i)
			}
			if secondPartUsesLeft && secondPartUsesRight {
				return nil, fmt.Errorf("right hand side of %d join predicate equality uses variables from both input tables", i)
			}

			if firstPartUsesLeft && secondPartUsesLeft {
				return nil, fmt.Errorf("both side of %d join predicate equality use variables from the left input table", i)
			}
			if firstPartUsesRight && secondPartUsesRight {
				return nil, fmt.Errorf("both side of %d join predicate equality use variables from the right input table", i)
			}

			if firstPartUsesLeft && secondPartUsesRight {
				leftKey[i] = firstPart
				rightKey[i] = secondPart
			} else if firstPartUsesRight && secondPartUsesLeft {
				leftKey[i] = secondPart
				rightKey[i] = firstPart
			}
		}

		leftKeyExprs := make([]execution.Expression, len(leftKey))
		for i := range leftKey {
			expr, err := leftKey[i].Materialize(ctx, env.WithRecordSchema(node.Join.Left.Schema))
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize stream join left key expression with index %d: %w", i, err)
			}
			leftKeyExprs[i] = expr
		}
		rightKeyExprs := make([]execution.Expression, len(rightKey))
		for i := range rightKey {
			expr, err := rightKey[i].Materialize(ctx, env.WithRecordSchema(node.Join.Right.Schema))
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize stream join right key expression with index %d: %w", i, err)
			}
			rightKeyExprs[i] = expr
		}

		return nodes.NewStreamJoin(left, right, leftKeyExprs, rightKeyExprs), nil
	case NodeTypeMap:
		source, err := node.Map.Source.Materialize(ctx, env)
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize map source: %w", err)
		}
		expressions := make([]execution.Expression, len(node.Map.Expressions))
		for i := range node.Map.Expressions {
			expr, err := node.Map.Expressions[i].Materialize(ctx, env.WithRecordSchema(node.Map.Source.Schema))
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize map expression with index %d: %w", i, err)
			}
			expressions[i] = expr
		}

		return nodes.NewMap(source, expressions), nil
	case NodeTypeOrderBy:
		source, err := node.OrderBy.Source.Materialize(ctx, env)
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize order by source: %w", err)
		}
		keyExprs := make([]execution.Expression, len(node.OrderBy.Key))
		for i := range node.OrderBy.Key {
			expr, err := node.OrderBy.Key[i].Materialize(ctx, env.WithRecordSchema(node.OrderBy.Source.Schema))
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize order by key with index %d: %w", i, err)
			}
			keyExprs[i] = expr
		}
		return nodes.NewBatchOrderBy(source, keyExprs, node.OrderBy.DirectionMultipliers), nil
	case NodeTypeRequalifier:
		return node.Requalifier.Source.Materialize(ctx, env)

	case NodeTypeTableValuedFunction:
		return node.TableValuedFunction.FunctionDescriptor.Materialize(ctx, env, node.TableValuedFunction.Arguments)
	}

	panic(fmt.Sprintf("unexhaustive node type match: %d", node.NodeType))
}

func usesVariablesFromLeftOrRight(left, right Schema, variables []string) (usesLeft bool, usesRight bool, err error) {
	for _, name := range variables {
		var matchedLeft, matchedRight bool
		for _, field := range left.Fields {
			if VariableNameMatchesField(name, field.Name) {
				usesLeft = true
				matchedLeft = true
				break
			}
		}
		for _, field := range right.Fields {
			if VariableNameMatchesField(name, field.Name) {
				usesRight = true
				matchedRight = true
				break
			}
		}
		if matchedLeft && matchedRight {
			return false, false, fmt.Errorf("ambiguous variable name in join predicate: %s", name)
		}
	}
	return
}
