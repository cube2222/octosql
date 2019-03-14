package physical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type Node interface {
	Materialize(ctx context.Context) (execution.Node, error)
}

type Expression interface {
	Materialize(ctx context.Context) (execution.Expression, error)
}

type NamedExpression interface {
	Expression
	MaterializeNamed(ctx context.Context) (execution.NamedExpression, error)
}

type Variable struct {
	name octosql.VariableName
}

func NewVariable(name octosql.VariableName) *Variable {
	return &Variable{name: name}
}

func (v *Variable) Materialize(ctx context.Context) (execution.Expression, error) {
	return v.MaterializeNamed(ctx)
}

func (v *Variable) MaterializeNamed(ctx context.Context) (execution.NamedExpression, error) {
	return execution.NewVariable(v.name), nil
}

type NodeExpression struct {
	Node Node
}

func NewNodeExpression(node Node) *NodeExpression {
	return &NodeExpression{Node: node}
}

func (ne *NodeExpression) Materialize(ctx context.Context) (execution.Expression, error) {
	materialized, err := ne.Node.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize node")
	}
	return execution.NewNodeExpression(materialized), nil
}

type AliasedExpression struct {
	name octosql.VariableName
	Expr Expression
}

func NewAliasedExpression(name octosql.VariableName, expr Expression) *AliasedExpression {
	return &AliasedExpression{name: name, Expr: expr}
}

func (alExpr *AliasedExpression) Materialize(ctx context.Context) (execution.Expression, error) {
	return alExpr.MaterializeNamed(ctx)
}

func (alExpr *AliasedExpression) MaterializeNamed(ctx context.Context) (execution.NamedExpression, error) {
	materialized, err := alExpr.Expr.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize node")
	}
	return execution.NewAliasedExpression(alExpr.name, materialized), nil
}
