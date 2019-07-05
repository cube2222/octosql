package app

import (
	"context"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/output"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/optimizer"
	"github.com/pkg/errors"
)

type App struct {
	dataSourceRepository *physical.DataSourceRepository
	out                  output.Output
}

func NewApp(dataSourceRepository *physical.DataSourceRepository, out output.Output) *App {
	return &App{
		dataSourceRepository: dataSourceRepository,
		out:                  out,
	}
}

func (app *App) RunPlan(ctx context.Context, plan logical.Node) error {
	phys, variables, err := plan.Physical(ctx, logical.NewPhysicalPlanCreator(app.dataSourceRepository))
	if err != nil {
		return errors.Wrap(err, "couldn't create physical plan")
	}

	phys = optimizer.Optimize(ctx, optimizer.DefaultScenarios, phys)

	exec, err := phys.Materialize(ctx)
	if err != nil {
		return errors.Wrap(err, "couldn't materialize the physical plan into an execution plan")
	}

	stream, err := exec.Get(variables)
	if err != nil {
		return errors.Wrap(err, "couldn't get record stream from execution plan")
	}

	var rec *execution.Record
	for rec, err = stream.Next(); err == nil; rec, err = stream.Next() {
		err := app.out.WriteRecord(rec)
		if err != nil {
			return errors.Wrap(err, "couldn't write record")
		}
	}
	if err != execution.ErrEndOfStream {
		return errors.Wrap(err, "couldn't get next record")
	}

	err = app.out.Close()
	if err != nil {
		return errors.Wrap(err, "couldn't close output writer")
	}

	return nil
}
