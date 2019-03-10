package physical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
)

type Node interface {
	Materialize(ctx context.Context) execution.Node
}

type Expression interface {
	Materialize(ctx context.Context) execution.Expression
}

type NamedExpression interface {
	Expression
	MaterializeNamed(ctx context.Context) execution.NamedExpression
}

type Variable struct {
	name octosql.VariableName
}

func NewVariable(name octosql.VariableName) *Variable {
	return &Variable{name: name}
}

func (v *Variable) Materialize(ctx context.Context) execution.Expression {
	return v.MaterializeNamed(ctx)
}

func (v *Variable) MaterializeNamed(ctx context.Context) execution.NamedExpression {
	return execution.NewVariable(v.name)
}

type NodeExpression struct {
	Node Node
}

func NewNodeExpression(node Node) *NodeExpression {
	return &NodeExpression{Node: node}
}

func (ne *NodeExpression) Materialize(ctx context.Context) execution.Expression {
	return execution.NewNodeExpression(ne.Node.Materialize(ctx))
}

type AliasedExpression struct {
	name octosql.VariableName
	Expr Expression
}

func NewAliasedExpression(name octosql.VariableName, expr Expression) *AliasedExpression {
	return &AliasedExpression{name: name, Expr: expr}
}

func (alExpr *AliasedExpression) Materialize(ctx context.Context) execution.Expression {
	return alExpr.MaterializeNamed(ctx)
}

func (alExpr *AliasedExpression) MaterializeNamed(ctx context.Context) execution.NamedExpression {
	return execution.NewAliasedExpression(alExpr.name, alExpr.Expr.Materialize(ctx))
}
