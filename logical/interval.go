package logical

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical"
)

type Interval struct {
	count Expression
	unit  Expression
}

func NewInterval(count Expression, unit Expression) *Interval {
	return &Interval{count: count, unit: unit}
}

func (v *Interval) Typecheck(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, octosql.Variables, error) {
	physCount, countVariables, err := v.count.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for count expression")
	}

	physUnit, unitVariables, err := v.unit.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for unit expression")
	}

	variables, err := countVariables.MergeWith(unitVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't merge count and unit variables")
	}

	return physical.NewFunctionExpression(
			"duration",
			[]physical.Expression{
				physCount,
				physUnit,
			},
		),
		variables,
		nil
}

func (v *Interval) Visualize() *graph.Node {
	n := graph.NewNode("Interval")
	if v.count != nil {
		n.AddChild("count", v.count.Visualize())
	}
	if v.unit != nil {
		n.AddChild("unit", v.unit.Visualize())
	}
	return n
}
