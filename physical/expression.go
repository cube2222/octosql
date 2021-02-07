package physical

import (
	"context"
	"fmt"
	"strings"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
)

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
	Name     string
	IsLevel0 bool
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

func (expr *Expression) Materialize(ctx context.Context, env Environment) (execution.Expression, error) {
	switch expr.ExpressionType {
	case ExpressionTypeVariable:
		level := 0
		index := 0
	ctxLoop:
		for varCtx := env.VariableContext; varCtx != nil; varCtx = varCtx.Parent {
			for i, field := range varCtx.Fields {
				if VariableNameMatchesField(expr.Variable.Name, field.Name) {
					index = i
					break ctxLoop
				}
			}
			level++
		}
		return execution.NewVariable(level, index), nil

	case ExpressionTypeConstant:
		return execution.NewConstant(expr.Constant.Value), nil
	case ExpressionTypeFunctionCall:
		panic("implement me")
	case ExpressionTypeAnd:
		expressions := make([]execution.Expression, len(expr.And.Arguments))
		for i := range expr.And.Arguments {
			expression, err := expr.And.Arguments[i].Materialize(ctx, env)
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize 'and' expression with index %d: %w", i, err)
			}
			expressions[i] = expression
		}
		return execution.NewAnd(expressions), nil
	case ExpressionTypeOr:
		expressions := make([]execution.Expression, len(expr.Or.Arguments))
		for i := range expr.Or.Arguments {
			expression, err := expr.Or.Arguments[i].Materialize(ctx, env)
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize 'or' expression with index %d: %w", i, err)
			}
			expressions[i] = expression
		}
		return execution.NewOr(expressions), nil
	case ExpressionTypeTypeAssertion:
		expression, err := expr.TypeAssertion.Expression.Materialize(ctx, env)
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize type asserted expression: %w", err)
		}

		return execution.NewTypeAssertion(expr.TypeAssertion.TargetType, expression), nil
	}

	panic("unexhaustive expression type match")
}

func VariableNameMatchesField(varName, fieldName string) bool {
	if varName == fieldName {
		return true
	}
	if !strings.Contains(varName, ".") &&
		strings.Contains(fieldName, ".") &&
		fieldName[strings.Index(fieldName, ".")+1:] == varName {
		return true
	}
	return false
}

func (expr Expression) SplitByAnd() []Expression {
	if expr.ExpressionType != ExpressionTypeAnd {
		return []Expression{expr}
	}
	var parts []Expression
	for _, arg := range expr.And.Arguments {
		parts = append(parts, arg.SplitByAnd()...)
	}
	return parts
}

func (expr Expression) VariablesUsed() []string {
	acc := make(map[string]struct{})
	expr.variablesUsed(acc)

	var out []string
	for k := range acc {
		out = append(out, k)
	}
	return out
}

func (expr Expression) variablesUsed(acc map[string]struct{}) {
	switch expr.ExpressionType {
	case ExpressionTypeVariable:
		acc[expr.Variable.Name] = struct{}{}
		return
	case ExpressionTypeConstant:
		return
	case ExpressionTypeFunctionCall:
		for _, arg := range expr.FunctionCall.Arguments {
			arg.variablesUsed(acc)
		}
		return
	case ExpressionTypeAnd:
		for _, arg := range expr.And.Arguments {
			arg.variablesUsed(acc)
		}
		return
	case ExpressionTypeOr:
		for _, arg := range expr.Or.Arguments {
			arg.variablesUsed(acc)
		}
		return
	case ExpressionTypeTypeAssertion:
		expr.TypeAssertion.Expression.variablesUsed(acc)
		return
	}

	panic("unexhaustive expression type match")
}
