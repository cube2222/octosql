package app

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/output"
	"github.com/cube2222/octosql/output/badger"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/optimizer"
	"github.com/cube2222/octosql/streaming/storage"

	"github.com/pkg/errors"
)

type App struct {
	cfg                  *config.Config
	dataSourceRepository *physical.DataSourceRepository
	out                  output.Output
	describe             bool
}

func NewApp(cfg *config.Config, dataSourceRepository *physical.DataSourceRepository, out output.Output, describe bool) *App {
	return &App{
		cfg:                  cfg,
		dataSourceRepository: dataSourceRepository,
		out:                  out,
		describe:             describe,
	}
}

func (app *App) RunPlan(ctx context.Context, stateStorage storage.Storage, plan logical.Node) error {
	sourceNodes, variables, err := plan.Physical(ctx, logical.NewPhysicalPlanCreator(app.dataSourceRepository, app.cfg.Physical))
	if err != nil {
		return errors.Wrap(err, "couldn't create physical plan")
	}

	// We only want one partition at the end, to print the output easily.
	shuffled := physical.NewShuffle(1, physical.NewConstantStrategy(0), sourceNodes)

	// Only the first partition is there.
	var phys physical.Node = shuffled[0]

	phys = optimizer.Optimize(ctx, optimizer.DefaultScenarios, phys)

	if app.describe {
		fmt.Print(graph.Show(phys.Visualize()).String())
		return nil
	}

	exec, err := phys.Materialize(ctx, physical.NewMaterializationContext(app.cfg, stateStorage))
	if err != nil {
		return errors.Wrap(err, "couldn't materialize the physical plan into an execution plan")
	}

	stream, execOutput, err := execution.GetAndStartAllShuffles(ctx, stateStorage, []execution.Node{exec}, variables)
	if err != nil {
		return errors.Wrap(err, "couldn't get record stream from execution plan")
	}

	out := &badger.Output{
		EventTimeField: phys.Metadata().EventTimeField(),
	}

	outStreamID := &execution.StreamID{Id: "output"}

	ctx, cancel := context.WithCancel(ctx)
	pullEngine := execution.NewPullEngine(out, stateStorage, stream[0], outStreamID, execOutput[0].WatermarkSource, true, cancel)

	go pullEngine.Run(ctx)

	printer := badger.NewStdOutPrinter(stateStorage.WithPrefix(outStreamID.AsPrefix()), out)
	if err := printer.Run(ctx); err != nil {
		return errors.Wrap(err, "couldn't run stdout printer")
	}

	return nil
}
