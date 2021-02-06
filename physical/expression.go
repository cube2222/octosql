package physical

import "github.com/cube2222/octosql"

type Expression struct {
	Type octosql.Type

	ExpressionType ExpressionType
	// Only one of the below may be non-null.
	Variable      *Variable
	Constant      *Constant
	FunctionCall  *FunctionCall
	And           *And
	Or            *Or
	TypeAssertion *TypeAssertion
}

type ExpressionType int

const (
	ExpressionTypeVariable ExpressionType = iota
	ExpressionTypeConstant
	ExpressionTypeFunctionCall
	ExpressionTypeAnd
	ExpressionTypeOr
	ExpressionTypeTypeAssertion
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

type TypeAssertion struct {
	Expression Expression
	TargetType octosql.Type
}
