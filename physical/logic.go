package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
)

type Formula interface {
	Materialize(ctx context.Context) execution.Formula
}

type Constant struct {
	Value bool
}

func NewConstant(value bool) *Constant {
	return &Constant{Value: value}
}

func (f *Constant) Materialize(ctx context.Context) execution.Formula {
	return execution.NewConstant(f.Value)
}

type And struct {
	Left, Right Formula
}

func NewAnd(left Formula, right Formula) *And {
	return &And{Left: left, Right: right}
}

func (f *And) Materialize(ctx context.Context) execution.Formula {
	return execution.NewAnd(f.Left.Materialize(ctx), f.Right.Materialize(ctx))
}

type Or struct {
	Left, Right Formula
}

func NewOr(left Formula, right Formula) *Or {
	return &Or{Left: left, Right: right}
}

func (f *Or) Materialize(ctx context.Context) execution.Formula {
	return execution.NewOr(f.Left.Materialize(ctx), f.Right.Materialize(ctx))
}

type Not struct {
	Child Formula
}

func NewNot(child Formula) *Not {
	return &Not{Child: child}
}

func (f *Not) Materialize(ctx context.Context) execution.Formula {
	return execution.NewNot(f.Child.Materialize(ctx))
}

type Predicate struct {
	Left     Expression
	Relation Relation
	Right    Expression
}

func NewPredicate(left Expression, relation Relation, right Expression) *Predicate {
	return &Predicate{Left: left, Relation: relation, Right: right}
}

func (f *Predicate) Materialize(ctx context.Context) execution.Formula {
	return execution.NewPredicate(f.Left.Materialize(ctx), f.Relation.Materialize(ctx), f.Right.Materialize(ctx))
}
