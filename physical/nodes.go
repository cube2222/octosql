package physical

import (
	"github.com/cube2222/octosql"
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
	Fields    []SchemaFields
	TimeField int
}

type SchemaFields struct {
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
	Name string
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
	Name string
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
