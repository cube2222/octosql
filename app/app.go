package app

import (
	"context"
	"fmt"
	"log"

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
	sourceNodes, variables, err := plan.Physical(ctx, logical.NewPhysicalPlanCreator(app.dataSourceRepository))
	if err != nil {
		return errors.Wrap(err, "couldn't create physical plan")
	}

	// We only want one partition at the end, to print the output easily.
	shuffled := physical.NewShuffle(1, sourceNodes, physical.DefaultShuffleStrategy)

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

	programID := &execution.StreamID{Id: "root"}

	tx := stateStorage.BeginTransaction()
	stream, execOutput, err := exec.Get(storage.InjectStateTransaction(ctx, tx), variables, programID)
	if err != nil {
		return errors.Wrap(err, "couldn't get record stream from execution plan")
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "couldn't commit transaction to get record stream from execution plan")
	}

	out := &badger.Output{
		EventTimeField: phys.Metadata().EventTimeField(),
	}

	outStreamID := &execution.StreamID{Id: "output"}

	pullEngine := execution.NewPullEngine(out, stateStorage, stream, outStreamID, execOutput.WatermarkSource)

	go func() {
		pullEngine.Run(ctx)
	}()

	printer := badger.NewStdOutPrinter(stateStorage.WithPrefix(outStreamID.AsPrefix()), out)
	log.Fatal(printer.Run(ctx))

	return nil
}
