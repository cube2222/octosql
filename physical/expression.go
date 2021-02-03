package physical

import "github.com/cube2222/octosql"

type Expression struct {
	Type octosql.Type

	ExpressionType ExpressionType
	// Only one of the below may be non-null.
	Variable     *Variable
	Constant     *Constant
	FunctionCall *FunctionCall
	And          *And
	Or           *Or
}

type ExpressionType int

const (
	ExpressionTypeFilter ExpressionType = iota
	ExpressionTypeGroupBy
	ExpressionTypeMap
	ExpressionTypeJoin
	ExpressionTypeTableValuedFunction
)

type Variable struct {
	Name string
}

type Constant struct {
	Value octosql.Value
}

type FunctionCall struct {
	Name      string
	Arguments []Expression
}

type And struct {
	Arguments []Expression
}

type Or struct {
	Arguments []Expression
}
