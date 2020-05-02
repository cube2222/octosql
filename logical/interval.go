package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type Interval struct {
	count Expression
	unit  Expression
}

func (interval *Interval) Visualize() *graph.Node {
	n := graph.NewNode("InnerJoin")
	if interval.count != nil {
		n.AddChild("count", interval.count.Visualize())
	}
	if interval.unit != nil {
		n.AddChild("unit", interval.unit.Visualize())
	}
	return n
}

func NewInterval(count Expression, unit Expression) *Interval {
	return &Interval{count: count, unit: unit}
}

func (v *Interval) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, octosql.Variables, error) {
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
