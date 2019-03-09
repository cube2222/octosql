package logical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

type Filter struct {
	formula Formula
	child   Node
}

func NewFilter(formula Formula, child Node) *Filter {
	return &Filter{formula: formula, child: child}
}

func (node *Filter) Physical(ctx context.Context, physicalCreator PhysicalPlanCreator) (physical.Node, octosql.Variables, error) {
	formula, formulaVariables, err := node.formula.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for formula")
	}
	child, childVariables, err := node.child.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for filter child node")
	}

	variables, err := childVariables.MergeWith(formulaVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't merge variables for filter child")
	}

	return physical.NewFilter(formula, child), variables, nil
}
