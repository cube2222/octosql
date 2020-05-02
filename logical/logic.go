package logical

import (
	"context"
	"fmt"
	"strings"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type Formula interface {
	graph.Visualizer

	Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Formula, octosql.Variables, error)
}

type BooleanConstant struct {
	Value bool
}

func (booleanConstant *BooleanConstant) Visualize() *graph.Node {
	n := graph.NewNode("BooleanConstant")
	n.AddField("Value", fmt.Sprint(booleanConstant.Value))
	return n
}

func NewBooleanConstant(value bool) *BooleanConstant {
	return &BooleanConstant{Value: value}
}

func (f *BooleanConstant) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Formula, octosql.Variables, error) {
	return physical.NewConstant(f.Value), octosql.NoVariables(), nil
}

type InfixOperator struct {
	Left     Formula
	Operator string
	Right    Formula
}

func (infixOperator *InfixOperator) Visualize() *graph.Node {
	n := graph.NewNode("InfixOperator")
	n.AddField("Operator", infixOperator.Operator)

	if infixOperator.Left != nil {
		n.AddChild("Left", infixOperator.Left.Visualize())
	}
	if infixOperator.Right != nil {
		n.AddChild("Right", infixOperator.Right.Visualize())
	}
	return n
}

func NewInfixOperator(left Formula, right Formula, operator string) *InfixOperator {
	return &InfixOperator{Left: left, Right: right, Operator: operator}
}

func (f *InfixOperator) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Formula, octosql.Variables, error) {
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

type PrefixOperator struct {
	Child    Formula
	Operator string
}

func (prefixOperator *PrefixOperator) Visualize() *graph.Node {
	n := graph.NewNode("PrefixOperator")
	n.AddField("Operator", prefixOperator.Operator)

	if prefixOperator.Child != nil {
		n.AddChild("Child", prefixOperator.Child.Visualize())
	}
	return n
}

func NewPrefixOperator(child Formula, operator string) *PrefixOperator {
	return &PrefixOperator{Child: child, Operator: operator}
}

func (f *PrefixOperator) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Formula, octosql.Variables, error) {
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

type Predicate struct {
	Left     Expression
	Relation Relation
	Right    Expression
}

func (predicate *Predicate) Visualize() *graph.Node {
	n := graph.NewNode("Predicate")
	n.AddField("Relation", string(predicate.Relation))

	if predicate.Left != nil {
		n.AddChild("Left", predicate.Left.Visualize())
	}
	if predicate.Right != nil {
		n.AddChild("Right", predicate.Right.Visualize())
	}
	return n
}

func NewPredicate(left Expression, relation Relation, right Expression) *Predicate {
	return &Predicate{Left: left, Relation: relation, Right: right}
}

func (f *Predicate) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Formula, octosql.Variables, error) {
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
