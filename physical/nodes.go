package physical

import (
	"context"
	"fmt"
	"strings"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/nodes"
	"github.com/cube2222/octosql/octosql"
)

type Node struct {
	Schema Schema

	NodeType NodeType
	// Only one of the below may be non-null.
	Datasource          *Datasource
	Distinct            *Distinct
	Filter              *Filter
	GroupBy             *GroupBy
	LookupJoin          *LookupJoin
	StreamJoin          *StreamJoin
	Map                 *Map
	OrderBy             *OrderBy
	Requalifier         *Requalifier
	TableValuedFunction *TableValuedFunction
	Unnest              *Unnest
	Limit               *Limit
}

type Schema struct {
	Fields []SchemaField
	// TimeField is -1 if not present.
	TimeField     int
	NoRetractions bool
}

type SchemaOption func(schema *Schema)

func NewSchema(fields []SchemaField, timeField int, options ...SchemaOption) Schema {
	out := Schema{
		Fields:    fields,
		TimeField: timeField,
	}
	for _, opt := range options {
		opt(&out)
	}
	return out
}

func WithNoRetractions(noRetractions bool) func(schema *Schema) {
	return func(schema *Schema) {
		schema.NoRetractions = noRetractions
	}
}

type SchemaField struct {
	Name string
	Type octosql.Type
}

type NodeType int

const (
	NodeTypeDatasource NodeType = iota
	NodeTypeDistinct
	NodeTypeFilter
	NodeTypeGroupBy
	NodeTypeLookupJoin
	NodeTypeStreamJoin
	NodeTypeMap
	NodeTypeOrderBy
	NodeTypeRequalifier
	NodeTypeTableValuedFunction
	NodeTypeUnnest
	NodeTypeLimit
)

func (t NodeType) String() string {
	switch t {
	case NodeTypeDatasource:
		return "datasource"
	case NodeTypeDistinct:
		return "distinct"
	case NodeTypeFilter:
		return "filter"
	case NodeTypeGroupBy:
		return "group_by"
	case NodeTypeLookupJoin:
		return "lookup_join"
	case NodeTypeStreamJoin:
		return "stream_join"
	case NodeTypeMap:
		return "map"
	case NodeTypeOrderBy:
		return "order_by"
	case NodeTypeRequalifier:
		return "requalifier"
	case NodeTypeTableValuedFunction:
		return "table_valued_function"
	case NodeTypeUnnest:
		return "unnest"
	case NodeTypeLimit:
		return "limit"
	}
	return "unknown"
}

type Datasource struct {
	Name, Alias              string
	DatasourceImplementation DatasourceImplementation
	VariableMapping          map[string]string
	Predicates               []Expression
}

func (node *Datasource) PushDownPredicates(newPredicates, pushedDownPredicates []Expression) (rejected []Expression, pushedDown []Expression, changed bool) {
	uniqueToColname := make(map[string]string)
	colnameToUnique := make(map[string]string)
	for k, v := range node.VariableMapping {
		trimmed := strings.TrimPrefix(k, node.Alias+".")
		uniqueToColname[v] = trimmed
		colnameToUnique[trimmed] = v
	}

	newPredicatesOriginalNames := renameExpressionSliceRecordVariables(uniqueToColname, newPredicates)
	pushedDownPredicatesOriginalNames := renameExpressionSliceRecordVariables(uniqueToColname, pushedDownPredicates)

	rejectedOriginalNames, pushedDownOriginalNames, changed := node.DatasourceImplementation.PushDownPredicates(newPredicatesOriginalNames, pushedDownPredicatesOriginalNames)

	rejected = renameExpressionSliceRecordVariables(colnameToUnique, rejectedOriginalNames)
	pushedDown = renameExpressionSliceRecordVariables(colnameToUnique, pushedDownOriginalNames)
	return rejected, pushedDown, changed
}

func renameExpressionSliceRecordVariables(oldToNew map[string]string, exprs []Expression) []Expression {
	out := make([]Expression, len(exprs))
	for i := range exprs {
		out[i] = renameRecordVariablesExpr(oldToNew, exprs[i])
	}
	return out
}

func renameRecordVariablesExpr(oldToNew map[string]string, expr Expression) Expression {
	t := Transformers{
		ExpressionTransformer: func(expr Expression) Expression {
			if expr.ExpressionType == ExpressionTypeVariable && expr.Variable.IsLevel0 {
				if newName, ok := oldToNew[expr.Variable.Name]; ok {
					expr.Variable.Name = newName
				}
			}
			return expr
		},
	}
	return t.TransformExpr(expr)
}

type Distinct struct {
	Source Node
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
	// Either index to Key, or -1, if none.
	KeyEventTimeIndex int
	Trigger           Trigger
}

type Aggregate struct {
	Name                string
	OutputType          octosql.Type
	AggregateDescriptor AggregateDescriptor
}

type StreamJoin struct {
	Left, Right       Node
	LeftKey, RightKey []Expression
}

type LookupJoin struct {
	Source, Joined Node
}

type Map struct {
	Source      Node
	Expressions []Expression
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

type TableValuedFunctionDescriptor struct {
	Materialize func(context.Context, Environment, map[string]TableValuedFunctionArgument) (execution.Node, error)
}

type Unnest struct {
	Source Node
	Field  string
}

type Limit struct {
	Source Node
	Limit  Expression
}

func (node *Node) Materialize(ctx context.Context, env Environment) (execution.Node, error) {
	switch node.NodeType {
	case NodeTypeDatasource:
		uniqueToColname := make(map[string]string)
		for k, v := range node.Datasource.VariableMapping {
			uniqueToColname[v] = strings.TrimPrefix(k, node.Datasource.Alias+".")
		}
		predicatesOriginalNames := renameExpressionSliceRecordVariables(uniqueToColname, node.Datasource.Predicates)

		fieldsOriginalNames := make([]SchemaField, len(node.Schema.Fields))
		for i := range node.Schema.Fields {
			fieldsOriginalNames[i] = SchemaField{
				Name: uniqueToColname[node.Schema.Fields[i].Name],
				Type: node.Schema.Fields[i].Type,
			}
		}
		schemaOriginalNames := NewSchema(fieldsOriginalNames, node.Schema.TimeField)

		return node.Datasource.DatasourceImplementation.Materialize(ctx, env, schemaOriginalNames, predicatesOriginalNames)
	case NodeTypeDistinct:
		source, err := node.Distinct.Source.Materialize(ctx, env)
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize distinct source: %w", err)
		}
		return nodes.NewDistinct(source), nil
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
		if node.GroupBy.Trigger.TriggerType == TriggerTypeEndOfStream {
			return nodes.NewSimpleGroupBy(aggregates, expressions, key, source), nil
		}
		trigger := node.GroupBy.Trigger.Materialize(ctx, env)

		return nodes.NewCustomTriggerGroupBy(aggregates, expressions, key, node.GroupBy.KeyEventTimeIndex, source, trigger), nil
	case NodeTypeStreamJoin:
		left, err := node.StreamJoin.Left.Materialize(ctx, env)
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize left join source: %w", err)
		}
		right, err := node.StreamJoin.Right.Materialize(ctx, env)
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize right join source: %w", err)
		}

		leftKeyExprs := make([]execution.Expression, len(node.StreamJoin.LeftKey))
		for i := range node.StreamJoin.LeftKey {
			expr, err := node.StreamJoin.LeftKey[i].Materialize(ctx, env.WithRecordSchema(node.StreamJoin.Left.Schema))
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize stream join left key expression with index %d: %w", i, err)
			}
			leftKeyExprs[i] = expr
		}
		rightKeyExprs := make([]execution.Expression, len(node.StreamJoin.RightKey))
		for i := range node.StreamJoin.RightKey {
			expr, err := node.StreamJoin.RightKey[i].Materialize(ctx, env.WithRecordSchema(node.StreamJoin.Right.Schema))
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize stream join right key expression with index %d: %w", i, err)
			}
			rightKeyExprs[i] = expr
		}

		return nodes.NewStreamJoin(left, right, leftKeyExprs, rightKeyExprs), nil
	case NodeTypeLookupJoin:
		source, err := node.LookupJoin.Source.Materialize(ctx, env)
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize left join source: %w", err)
		}
		joined, err := node.LookupJoin.Joined.Materialize(ctx, env.WithRecordSchema(node.LookupJoin.Source.Schema))
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize right join source: %w", err)
		}

		return nodes.NewLookupJoin(source, joined), nil
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

	case NodeTypeUnnest:
		source, err := node.Unnest.Source.Materialize(ctx, env)
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize unnest source: %w", err)
		}

		index := -1
		// This must be found, we are past timecheck phase
		for i, field := range node.Schema.Fields {
			if node.Unnest.Field == field.Name {
				index = i
				break
			}
		}
		if index == -1 {
			panic(fmt.Sprintf("unnest field '%s' not found, should be caught in typecheck", node.Unnest.Field))
		}

		return nodes.NewUnnest(source, index), nil

	case NodeTypeLimit:
		source, err := node.Limit.Source.Materialize(ctx, env)
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize limit source: %w", err)
		}
		limit, err := node.Limit.Limit.Materialize(ctx, env)
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize limit expression: %w", err)
		}
		return nodes.NewLimit(source, limit), nil
	}

	panic(fmt.Sprintf("unexhaustive node type match: %d", node.NodeType))
}
