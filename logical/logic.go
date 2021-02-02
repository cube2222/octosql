package logical

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical"
)

type Formula interface {
	graph.Visualizer

	Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Formula, octosql.Variables, error)
}

type BooleanConstant struct {
	Value bool
}

func NewBooleanConstant(value bool) *BooleanConstant {
	return &BooleanConstant{Value: value}
}

func (f *BooleanConstant) Typecheck(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Formula, octosql.Variables, error) {
	return physical.NewConstant(f.Value), octosql.NoVariables(), nil
}

func (f *BooleanConstant) Visualize() *graph.Node {
	n := graph.NewNode("BooleanConstant")
	n.AddField("Value", fmt.Sprint(f.Value))
	return n
}

type InfixOperator struct {
	Left     Formula
	Operator string
	Right    Formula
}

func NewInfixOperator(left Formula, right Formula, operator string) *InfixOperator {
	return &InfixOperator{Left: left, Right: right, Operator: operator}
}

func (f *InfixOperator) Typecheck(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Formula, octosql.Variables, error) {
	left, leftVariables, err := f.Left.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for left operand")
	}
	right, rightVariables, err := f.Right.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for right operand")
	}

	variables, err := leftVariables.MergeWith(rightVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get variables for operands")
	}

	switch strings.ToLower(f.Operator) {
	case "or":
		return physical.NewOr(left, right), variables, nil
	case "and":
		return physical.NewAnd(left, right), variables, nil
	default:
		return nil, nil, errors.Wrapf(err, "invalid logic infix operator %v", f.Operator)
	}
}

func (f *InfixOperator) Visualize() *graph.Node {
	n := graph.NewNode("Infix Operator")
	n.AddField("Operator", f.Operator)

	if f.Left != nil {
		n.AddChild("Left", f.Left.Visualize())
	}
	if f.Right != nil {
		n.AddChild("Right", f.Right.Visualize())
	}
	return n
}

type PrefixOperator struct {
	Child    Formula
	Operator string
}

func NewPrefixOperator(child Formula, operator string) *PrefixOperator {
	return &PrefixOperator{Child: child, Operator: operator}
}

func (f *PrefixOperator) Typecheck(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Formula, octosql.Variables, error) {
	child, variables, err := f.Child.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for operand")
	}

	switch strings.ToLower(f.Operator) {
	case "not":
		return physical.NewNot(child), variables, nil
	default:
		return nil, nil, errors.Wrapf(err, "invalid logic prefix operator %v", f.Operator)
	}
}

func (f *PrefixOperator) Visualize() *graph.Node {
	n := graph.NewNode("Prefix Operator")
	n.AddField("Operator", f.Operator)

	if f.Child != nil {
		n.AddChild("Child", f.Child.Visualize())
	}
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

func (f *Predicate) Typecheck(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Formula, octosql.Variables, error) {
	left, leftVariables, err := f.Left.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for left operand")
	}
	relation, err := f.Relation.Physical(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for relation")
	}
	right, rightVariables, err := f.Right.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for right operand")
	}

	variables, err := leftVariables.MergeWith(rightVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get variables for operands")
	}

	return physical.NewPredicate(left, relation, right), variables, nil
}

func (f *Predicate) Visualize() *graph.Node {
	n := graph.NewNode("Predicate")
	n.AddField("Relation", string(f.Relation))

	if f.Left != nil {
		n.AddChild("Left", f.Left.Visualize())
	}
	if f.Right != nil {
		n.AddChild("Right", f.Right.Visualize())
	}
	return n
}
