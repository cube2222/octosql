package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

// Formula describes any source of a logical value.
type Formula interface {
	// Transform returns a new Formula after recursively calling Transform
	Transform(ctx context.Context, transformers *Transformers) Formula
	Materialize(ctx context.Context) (execution.Formula, error)
}

type Constant struct {
	Value bool
}

func NewConstant(value bool) *Constant {
	return &Constant{Value: value}
}

func (f *Constant) Transform(ctx context.Context, transformers *Transformers) Formula {
	var formula Formula = &Constant{
		Value: f.Value,
	}
	if transformers.FormulaT != nil {
		formula = transformers.FormulaT(formula)
	}
	return formula
}

func (f *Constant) Materialize(ctx context.Context) (execution.Formula, error) {
	return execution.NewConstant(f.Value), nil
}

type And struct {
	Left, Right Formula
}

func NewAnd(left Formula, right Formula) *And {
	return &And{Left: left, Right: right}
}

func (f *And) Transform(ctx context.Context, transformers *Transformers) Formula {
	var formula Formula = &And{
		Left:  f.Left.Transform(ctx, transformers),
		Right: f.Right.Transform(ctx, transformers),
	}
	if transformers.FormulaT != nil {
		formula = transformers.FormulaT(formula)
	}
	return formula
}

func (f *And) Materialize(ctx context.Context) (execution.Formula, error) {
	materializedLeft, err := f.Left.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize left operand")
	}
	materializedRight, err := f.Right.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize right operand")
	}
	return execution.NewAnd(materializedLeft, materializedRight), nil
}

type Or struct {
	Left, Right Formula
}

func NewOr(left Formula, right Formula) *Or {
	return &Or{Left: left, Right: right}
}

func (f *Or) Transform(ctx context.Context, transformers *Transformers) Formula {
	var formula Formula = &Or{
		Left:  f.Left.Transform(ctx, transformers),
		Right: f.Right.Transform(ctx, transformers),
	}
	if transformers.FormulaT != nil {
		formula = transformers.FormulaT(formula)
	}
	return formula
}

func (f *Or) Materialize(ctx context.Context) (execution.Formula, error) {
	materializedLeft, err := f.Left.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize left operand")
	}
	materializedRight, err := f.Right.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize right operand")
	}
	return execution.NewOr(materializedLeft, materializedRight), nil
}

type Not struct {
	Child Formula
}

func NewNot(child Formula) *Not {
	return &Not{Child: child}
}

func (f *Not) Transform(ctx context.Context, transformers *Transformers) Formula {
	var formula Formula = &Not{
		Child: f.Child.Transform(ctx, transformers),
	}
	if transformers.FormulaT != nil {
		formula = transformers.FormulaT(formula)
	}
	return formula
}

func (f *Not) Materialize(ctx context.Context) (execution.Formula, error) {
	materialized, err := f.Child.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize operand")
	}
	return execution.NewNot(materialized), nil
}

type Predicate struct {
	Left     Expression
	Relation Relation
	Right    Expression
}

func NewPredicate(left Expression, relation Relation, right Expression) *Predicate {
	return &Predicate{Left: left, Relation: relation, Right: right}
}

func (f *Predicate) Transform(ctx context.Context, transformers *Transformers) Formula {
	var formula Formula = &Predicate{
		Left:     f.Left.Transform(ctx, transformers),
		Relation: f.Relation,
		Right:    f.Right.Transform(ctx, transformers),
	}
	if transformers.FormulaT != nil {
		formula = transformers.FormulaT(formula)
	}
	return formula
}

func (f *Predicate) Materialize(ctx context.Context) (execution.Formula, error) {
	materializedLeft, err := f.Left.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize left operand")
	}
	materializedRight, err := f.Right.Materialize(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize right operand")
	}
	return execution.NewPredicate(materializedLeft, f.Relation.Materialize(ctx), materializedRight), nil
}
