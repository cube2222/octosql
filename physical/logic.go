package physical

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical/metadata"

	"github.com/pkg/errors"
)

// Formula describes any source of a logical value.
type Formula interface {
	// Transform returns a new Formula after recursively calling Transform
	Transform(ctx context.Context, transformers *Transformers) Formula
	SplitByAnd() []Formula
	ExtractPredicates() []*Predicate
	Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Formula, error)
	DoesMatchNamespace(namespace *metadata.Namespace) bool
	graph.Visualizer
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

func (f *Constant) SplitByAnd() []Formula {
	return []Formula{f}
}

func (f *Constant) ExtractPredicates() []*Predicate {
	return []*Predicate{}
}

func (f *Constant) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Formula, error) {
	return execution.NewConstant(f.Value), nil
}

func (f *Constant) DoesMatchNamespace(namespace *metadata.Namespace) bool {
	return true
}

func (f *Constant) Visualize() *graph.Node {
	n := graph.NewNode("Constant")
	n.AddField("value", fmt.Sprint(f.Value))
	return n
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

func (f *And) SplitByAnd() []Formula {
	return append(f.Left.SplitByAnd(), f.Right.SplitByAnd()...)
}

func (f *And) ExtractPredicates() []*Predicate {
	return append(f.Left.ExtractPredicates(), f.Right.ExtractPredicates()...)
}

func (f *And) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Formula, error) {
	materializedLeft, err := f.Left.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize left operand")
	}
	materializedRight, err := f.Right.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize right operand")
	}
	return execution.NewAnd(materializedLeft, materializedRight), nil
}

func (f *And) DoesMatchNamespace(namespace *metadata.Namespace) bool {
	return f.Left.DoesMatchNamespace(namespace) && f.Right.DoesMatchNamespace(namespace)
}

func (f *And) Visualize() *graph.Node {
	n := graph.NewNode("And")
	n.AddChild("left", f.Left.Visualize())
	n.AddChild("right", f.Right.Visualize())
	return n
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

func (f *Or) SplitByAnd() []Formula {
	return []Formula{f}
}

func (f *Or) ExtractPredicates() []*Predicate {
	return append(f.Left.ExtractPredicates(), f.Right.ExtractPredicates()...)
}

func (f *Or) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Formula, error) {
	materializedLeft, err := f.Left.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize left operand")
	}
	materializedRight, err := f.Right.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize right operand")
	}
	return execution.NewOr(materializedLeft, materializedRight), nil
}

func (f *Or) DoesMatchNamespace(namespace *metadata.Namespace) bool {
	return f.Left.DoesMatchNamespace(namespace) && f.Right.DoesMatchNamespace(namespace)
}

func (f *Or) Visualize() *graph.Node {
	n := graph.NewNode("Or")
	n.AddChild("left", f.Left.Visualize())
	n.AddChild("right", f.Right.Visualize())
	return n
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

func (f *Not) SplitByAnd() []Formula {
	return []Formula{f}
}

func (f *Not) ExtractPredicates() []*Predicate {
	return f.Child.ExtractPredicates()
}

func (f *Not) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Formula, error) {
	materialized, err := f.Child.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize operand")
	}
	return execution.NewNot(materialized), nil
}

func (f *Not) DoesMatchNamespace(namespace *metadata.Namespace) bool {
	return f.Child.DoesMatchNamespace(namespace)
}

func (f *Not) Visualize() *graph.Node {
	n := graph.NewNode("Not")
	n.AddChild("source", f.Child.Visualize())
	return n
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

func (f *Predicate) SplitByAnd() []Formula {
	return []Formula{f}
}

func (f *Predicate) ExtractPredicates() []*Predicate {
	return []*Predicate{f}
}

func (f *Predicate) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Formula, error) {
	materializedLeft, err := f.Left.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize left operand")
	}
	materializedRight, err := f.Right.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize right operand")
	}
	return execution.NewPredicate(materializedLeft, f.Relation.Materialize(ctx, matCtx), materializedRight), nil
}

func (f *Predicate) DoesMatchNamespace(namespace *metadata.Namespace) bool {
	return f.Left.DoesMatchNamespace(namespace) && f.Right.DoesMatchNamespace(namespace)
}

func (f *Predicate) Visualize() *graph.Node {
	n := graph.NewNode("Predicate")
	n.AddField("relation", string(f.Relation))
	n.AddChild("left", f.Left.Visualize())
	n.AddChild("right", f.Right.Visualize())
	return n
}
