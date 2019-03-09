package execution

import (
	"github.com/cube2222/octosql"
)

type Formula interface {
	Evaluate(variables octosql.Variables) (bool, error)
}

type Constant struct {
	Value bool
}

func NewConstant(value bool) *Constant {
	return &Constant{Value: value}
}

func (f Constant) Evaluate(variables octosql.Variables) (bool, error) {
	return c.Value, nil
}

// TODO: Implement:

type And struct {
	Left, Right Formula
}

func NewAnd(left Formula, right Formula) *And {
	return &And{Left: left, Right: right}
}

func (f *And) Evaluate(variables octosql.Variables) (bool, error) {
	panic("implement me")
}

type Or struct {
	Left, Right Formula
}

func NewOr(left Formula, right Formula) *Or {
	return &Or{Left: left, Right: right}
}

func (f *Or) Evaluate(variables octosql.Variables) (bool, error) {
	panic("implement me")
}

type Not struct {
	Child Formula
}

func NewNot(child Formula) *Not {
	return &Not{Child: child}
}

func (f *Not) Evaluate(variables octosql.Variables) (bool, error) {
	panic("implement me")
}

type Predicate struct {
	Left     Expression
	Relation Relation
	Right    Expression
}

func NewPredicate(left Expression, relation Relation, right Expression) *Predicate {
	return &Predicate{Left: left, Relation: relation, Right: right}
}

func (f *Predicate) Evaluate(variables octosql.Variables) (bool, error) {
	return p.Filter.Apply(p.Left, p.Right)
}
