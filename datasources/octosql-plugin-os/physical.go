package main

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
)

type processesPhysical struct {
}

func (i *processesPhysical) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (execution.Node, error) {
	return &processesExecuting{
		fields: schema.Fields,
	}, nil
}

func (i *processesPhysical) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	// TODO: Allow getting by pid.
	return newPredicates, []physical.Expression{}, false
}
