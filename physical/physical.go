package physical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

// Transformers is a structure containing functions to transform each of the physical plan components.
type Transformers struct {
	NodeT      func(Node) Node
	ExprT      func(Expression) Expression
	NamedExprT func(NamedExpression) NamedExpression
	FormulaT   func(Formula) Formula
}

// Node describes a single record stream source.
type Node interface {
	// Transform returns a new Node after recursively calling Transform
	Transform(ctx context.Context, transformers *Transformers) Node
	Materialize(ctx context.Context) (execution.Node, error)
}

// Expressions describes a single value source.
type Expression interface {
	// Transform returns a new Expression after recursively calling Transform
	Transform(ctx context.Context, transformers *Transformers) Expression
	Materialize(ctx context.Context) (execution.Expression, error)
}

// NamedExpressions describes a single named value source.
type NamedExpression interface {
	Expression
	// TransformNamed returns a new NamedExpression after recursively calling Transform
	TransformNamed(ctx context.Context, transformers *Transformers) NamedExpression
	MaterializeNamed(ctx context.Context) (execution.NamedExpression, error)
}

// Variables describes a variable Name.
type Variable struct {
	Name octosql.VariableName
}

func NewVariable(name octosql.VariableName) *Variable {
	return &Variable{Name: name}
}

func (v *Variable) Transform(ctx context.Context, transformers *Transformers) Expression {
	return v.TransformNamed(ctx, transformers)
}

func (v *Variable) Materialize(ctx context.Context) (execution.Expression, error) {
	return v.MaterializeNamed(ctx)
}

func (v *Variable) TransformNamed(ctx context.Context, transformers *Transformers) NamedExpression {
	var expr NamedExpression = &Variable{
		Name: v.Name,
	}
	if transformers.NamedExprT != nil {
		expr = transformers.NamedExprT(expr)
	}
	return expr
}

func (v *Variable) MaterializeNamed(ctx context.Context) (execution.NamedExpression, error) {
	return execution.NewVariable(v.Name), nil
}

// TupleExpression describes an expression which is a tuple of subexpressions.
type Tuple struct {
	Expressions []Expression
}

func NewTuple(expressions []Expression) *Tuple {
	return &Tuple{Expressions: expressions}
}

func (tup *Tuple) Transform(ctx context.Context, transformers *Transformers) Expression {
	exprs := make([]Expression, len(tup.Expressions))
	for i := range tup.Expressions {
		exprs[i] = tup.Expressions[i].Transform(ctx, transformers)
	}
	var transformed Expression = &Tuple{
		Expressions: exprs,
	}
	if transformers.ExprT != nil {
		transformed = transformers.ExprT(transformed)
	}
	return transformed
}

func (tup *Tuple) Materialize(ctx context.Context) (execution.Expression, error) {
	matExprs := make([]execution.Expression, len(tup.Expressions))
	for i := range tup.Expressions {
		materialized, err := tup.Expressions[i].Materialize(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize expression with index %v", i)
		}
		matExprs[i] = materialized
	}

	return execution.NewTuple(matExprs), nil
}

// NodeExpressions describes an expression which gets it's value from a node underneath.
type NodeExpression struct {
	Node Node
}

func NewNodeExpression(node Node) *NodeExpression {
	return &NodeExpression{Node: node}
}

func (ne *NodeExpression) Transform(ctx context.Context, transformers *Transformers) Expression {
	var expr Expression = &NodeExpression{
		Node: ne.Node.Transform(ctx, transformers),
	}
	if transformers.ExprT != nil {
		expr = transformers.ExprT(expr)
	}
	return expr
}

func (ne *NodeExpression) Materialize(ctx context.Context) (execution.Expression, error) {
	materialized, err := ne.Node.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize node")
	}
	return execution.NewNodeExpression(materialized), nil
}

// AliasedExpression describes an expression which is explicitly named.
type AliasedExpression struct {
	Name octosql.VariableName
	Expr Expression
}

func NewAliasedExpression(name octosql.VariableName, expr Expression) *AliasedExpression {
	return &AliasedExpression{Name: name, Expr: expr}
}

func (alExpr *AliasedExpression) Transform(ctx context.Context, transformers *Transformers) Expression {
	return alExpr.TransformNamed(ctx, transformers)
}

func (alExpr *AliasedExpression) Materialize(ctx context.Context) (execution.Expression, error) {
	return alExpr.MaterializeNamed(ctx)
}

func (alExpr *AliasedExpression) TransformNamed(ctx context.Context, transformers *Transformers) NamedExpression {
	var expr NamedExpression = &AliasedExpression{
		Name: alExpr.Name,
		Expr: alExpr.Expr.Transform(ctx, transformers),
	}
	if transformers.NamedExprT != nil {
		expr = transformers.NamedExprT(expr)
	}
	return expr
}

func (alExpr *AliasedExpression) MaterializeNamed(ctx context.Context) (execution.NamedExpression, error) {
	materialized, err := alExpr.Expr.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize node")
	}
	return execution.NewAliasedExpression(alExpr.Name, materialized), nil
}
