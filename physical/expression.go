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
	Variable        *Variable
	Constant        *Constant
	RecordEventTime *RecordEventTime
	FunctionCall    *FunctionCall
	And             *And
	Or              *Or
	QueryExpression *QueryExpression
	Coalesce        *Coalesce
	Tuple           *Tuple
	TypeAssertion   *TypeAssertion
}

type ExpressionType int

const (
	ExpressionTypeVariable ExpressionType = iota
	ExpressionTypeConstant
	ExpressionTypeRecordEventTime
	ExpressionTypeFunctionCall
	ExpressionTypeAnd
	ExpressionTypeOr
	ExpressionTypeQueryExpression
	ExpressionTypeCoalesce
	ExpressionTypeTuple
	ExpressionTypeTypeAssertion
)

type Variable struct {
	Name     string
	IsLevel0 bool
}

type Constant struct {
	Value octosql.Value
}

type RecordEventTime struct {
}

type FunctionCall struct {
	Name               string
	Arguments          []Expression
	FunctionDescriptor FunctionDescriptor
}

type And struct {
	Arguments []Expression
}

type Or struct {
	Arguments []Expression
}

type QueryExpression struct {
	Source Node
}

type Coalesce struct {
	Arguments []Expression
}

type Tuple struct {
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
	case ExpressionTypeRecordEventTime:
		return execution.NewRecordEventTime(), nil
	case ExpressionTypeFunctionCall:
		expressions := make([]execution.Expression, len(expr.FunctionCall.Arguments))
		for i := range expr.FunctionCall.Arguments {
			expression, err := expr.FunctionCall.Arguments[i].Materialize(ctx, env)
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize function argument with index %d: %w", i, err)
			}
			expressions[i] = expression
		}
		var nullCheckIndices []int
		if expr.FunctionCall.FunctionDescriptor.Strict {
			for i := range expr.FunctionCall.Arguments {
				if octosql.Null.Is(expr.FunctionCall.Arguments[i].Type) == octosql.TypeRelationIs {
					nullCheckIndices = append(nullCheckIndices, i)
				}
			}
		}
		return execution.NewFunctionCall(expr.FunctionCall.FunctionDescriptor.Function, expressions, nullCheckIndices), nil
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
	case ExpressionTypeQueryExpression:
		source, err := expr.QueryExpression.Source.Materialize(ctx, env)
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize query expression source: %w", err)
		}
		singleColumn := false
		if len(expr.QueryExpression.Source.Schema.Fields) == 1 {
			singleColumn = true
		}

		if !singleColumn {
			fieldNames := make([]string, len(expr.QueryExpression.Source.Schema.Fields))
			for i := range expr.QueryExpression.Source.Schema.Fields {
				fieldNames[i] = expr.QueryExpression.Source.Schema.Fields[i].Name
			}
			return execution.NewMultiColumnQueryExpression(source, fieldNames), nil
		} else {
			return execution.NewSingleColumnQueryExpression(source), nil
		}
	case ExpressionTypeCoalesce:
		expressions := make([]execution.Expression, len(expr.Coalesce.Arguments))
		for i := range expr.Coalesce.Arguments {
			expression, err := expr.Coalesce.Arguments[i].Materialize(ctx, env)
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize COALESCE argument with index %d: %w", i, err)
			}
			expressions[i] = expression
		}
		return execution.NewCoalesce(expressions), nil
	case ExpressionTypeTuple:
		expressions := make([]execution.Expression, len(expr.Tuple.Arguments))
		for i := range expr.Tuple.Arguments {
			expression, err := expr.Tuple.Arguments[i].Materialize(ctx, env)
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize tuple argument with index %d: %w", i, err)
			}
			expressions[i] = expression
		}
		return execution.NewTuple(expressions), nil
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
	case ExpressionTypeRecordEventTime:
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
