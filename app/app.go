package app

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/output/badger"
	"github.com/cube2222/octosql/output/badger/table"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/optimizer"
	"github.com/cube2222/octosql/storage"

	"github.com/pkg/errors"
)

type App struct {
	cfg                  *config.Config
	dataSourceRepository *physical.DataSourceRepository
	out                  interface{}
	describe             bool
}

func NewApp(cfg *config.Config, dataSourceRepository *physical.DataSourceRepository, out interface{}, describe bool) *App {
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

	stream, execOutput, err := execution.GetAndStartAllShuffles(ctx, stateStorage, execution.NewStreamID("root"), []execution.Node{exec}, variables)
	if err != nil {
		return errors.Wrap(err, "couldn't get record stream from execution plan")
	}

	// out := &badger.TableOutput{
	// 	//EventTimeField: phys.Metadata().EventTimeField(),
	// }
	//
	// outStreamID := &execution.StreamID{Id: "output"}
	//
	// log.Printf("%T", execOutput[0].WatermarkSource)
	// pullEngine := execution.NewPullEngine(out, stateStorage, []execution.RecordStream{stream[0]}, outStreamID, execOutput[0].WatermarkSource, true, ctx)
	//
	// go pullEngine.Run()

	// printer := streaming.NewStreamPrinter(stateStorage, pullEngine, streaming.JSONPrinter())
	// if err := printer.Run(ctx); err != nil {
	// 	return errors.Wrap(err, "couldn't run stdout printer")
	// }

	out := &badger.TableOutput{
		EventTimeField: phys.Metadata().EventTimeField(),
	}

	outStreamID := &execution.StreamID{Id: "output"}

	pullEngine := execution.NewPullEngine(out, stateStorage, []execution.RecordStream{stream[0]}, outStreamID, execOutput[0].WatermarkSource, true, ctx)

	go pullEngine.Run()

	printer := badger.NewWholeTablePrinter(stateStorage.WithPrefix(outStreamID.AsPrefix()), out, table.TableFormatter(false))
	if err := printer.Run(ctx); err != nil {
		return errors.Wrap(err, "couldn't run stdout printer")
	}

	if err := pullEngine.Close(ctx, stateStorage); err != nil {
		return errors.Wrap(err, "couldn't close output pull engine")
	}

	return nil
}
