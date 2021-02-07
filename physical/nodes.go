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
	NodeTypeRequalifier
	NodeTypeTableValuedFunction
)

type Datasource struct {
	Name                     string
	DatasourceImplementation DatasourceImplementation
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

type Requalifier struct {
	Source    Node
	Qualifier string
}

type TableValuedFunction struct {
	Name      string
	Arguments []TableValuedFunctionArgument
}

type TableValuedFunctionArgument struct {
	Name *string

	TableValuedFunctionArgumentType TableValuedFunctionArgumentType
	// Only one of the below may be non-null.
	TableValuedFunctionArgumentExpression *TableValuedFunctionArgumentExpression
	TableValuedFunctionArgumentTable      *TableValuedFunctionArgumentTable
	TableValuedFunctionArgumentDescriptor *TableValuedFunctionArgumentDescriptor
}

type TableValuedFunctionArgumentType int

const (
	TableValuedFunctionArgumentTypeExpression TableValuedFunctionArgumentType = iota
	TableValuedFunctionArgumentTypeTable
	TableValuedFunctionArgumentTypeDescriptor
)

type TableValuedFunctionArgumentExpression struct {
}

type TableValuedFunctionArgumentTable struct {
}

type TableValuedFunctionArgumentDescriptor struct {
}

func (node *Node) Materialize(ctx context.Context, env Environment) (execution.Node, error) {
	switch node.NodeType {
	case NodeTypeDatasource:
		return node.Datasource.DatasourceImplementation.Materialize(ctx, env)
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
		panic("implement me")
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
	case NodeTypeRequalifier:
		return node.Requalifier.Source.Materialize(ctx, env)

	case NodeTypeTableValuedFunction:
		panic("implement me")
	}

	panic("unexhaustive node type match")
}
