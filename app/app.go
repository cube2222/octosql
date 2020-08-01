package app

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/output"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/optimizer"
	"github.com/cube2222/octosql/storage"

	"github.com/pkg/errors"
)

type OutputSinkFn func(stateStorage storage.Storage, streamID *execution.StreamID, eventTimeField octosql.VariableName, outputOptions *OutputOptions) (execution.IntermediateRecordStore, output.Printer)

type OutputOptions struct {
	OrderByExpressions []execution.Expression
	OrderByDirections  []execution.OrderDirection
	Limit              *int
	Offset             *int
}

func EvaluateOutputOptions(ctx context.Context, variables octosql.Variables, options *execution.OutputOptions) (*OutputOptions, error) {
	var limit *int
	if options.Limit != nil {
		limitValue, err := options.Limit.ExpressionValue(ctx, variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't evaluate limit expression")
		}
		limitInt := limitValue.AsInt()
		limit = &limitInt
	}

	var offset *int
	if options.Offset != nil {
		offsetValue, err := options.Offset.ExpressionValue(ctx, variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't evaluate offset expression")
		}
		offsetInt := offsetValue.AsInt()
		offset = &offsetInt
	}

	return &OutputOptions{
		OrderByExpressions: options.OrderByExpressions,
		OrderByDirections:  options.OrderByDirections,
		Limit:              limit,
		Offset:             offset,
	}, nil
}

type App struct {
	telemetryInfo        TelemetryInfo
	cfg                  *config.Config
	dataSourceRepository *physical.DataSourceRepository
	outputSinkFn         OutputSinkFn
	describe             bool
}

func NewApp(cfg *config.Config, telemetryInfo TelemetryInfo, dataSourceRepository *physical.DataSourceRepository, outputSinkFn OutputSinkFn, describe bool) *App {
	return &App{
		cfg:                  cfg,
		dataSourceRepository: dataSourceRepository,
		outputSinkFn:         outputSinkFn,
		describe:             describe,
		telemetryInfo:        telemetryInfo,
	}
}

func (app *App) RunPlan(ctx context.Context, stateStorage storage.Storage, plan logical.Node, outputOptions *logical.OutputOptions) error {
	physicalPlanCreator := logical.NewPhysicalPlanCreator(app.dataSourceRepository, app.cfg.Physical)
	sourceNodes, variables, err := plan.Physical(ctx, physicalPlanCreator)
	if err != nil {
		return errors.Wrap(err, "couldn't create physical plan")
	}
	physicalOutputOptions, outputOptionsVariables, err := outputOptions.Physical(ctx, physicalPlanCreator)
	if err != nil {
		return errors.Wrap(err, "couldn't create physical output options")
	}
	variables, err = variables.MergeWith(outputOptionsVariables)
	if err != nil {
		return errors.Wrap(err, "couldn't merge variables with output options variables")
	}

	// We only want one partition at the end, to print the output easily.
	shuffled := physical.NewShuffle(1, physical.NewConstantStrategy(0), sourceNodes)

	// Only the first partition is there.
	var phys physical.Node = shuffled[0]

	if strings.TrimSpace(os.Getenv("OCTOSQL_TELEMETRY")) != "0" {
		RunTelemetry(ctx, app.telemetryInfo, app.cfg.DataSources, phys, physicalOutputOptions)
	}

	phys = optimizer.Optimize(ctx, optimizer.DefaultScenarios, phys)

	if app.describe {
		fmt.Print(graph.Show(phys.Visualize()).String())
		return nil
	}

	matCtx := physical.NewMaterializationContext(app.cfg, stateStorage)
	exec, err := phys.Materialize(ctx, matCtx)
	if err != nil {
		return errors.Wrap(err, "couldn't materialize the physical plan into an execution plan")
	}
	execOutputOptions, err := physicalOutputOptions.Materialize(ctx, matCtx)
	if err != nil {
		return errors.Wrap(err, "couldn't materialize output options")
	}
	evalOutputOptions, err := EvaluateOutputOptions(ctx, variables, execOutputOptions)
	if err != nil {
		return errors.Wrap(err, "couldn't get output options")
	}

	stream, execOutput, err := execution.GetAndStartAllShuffles(ctx, stateStorage, execution.NewStreamID("root"), []execution.Node{exec}, variables)
	if err != nil {
		return errors.Wrap(err, "couldn't get record stream from execution plan")
	}

	outStreamID := &execution.StreamID{Id: "output"}

	outputSink, printer := app.outputSinkFn(stateStorage, outStreamID, phys.Metadata().EventTimeField(), evalOutputOptions)

	pullEngine := execution.NewPullEngine(outputSink, stateStorage, []execution.RecordStream{stream[0]}, outStreamID, execOutput[0].WatermarkSource, false, ctx)
	go pullEngine.Run()

	if err := printer.Run(ctx); err != nil {
		return errors.Wrap(err, "couldn't run stdout printer")
	}

	if err := pullEngine.Close(ctx, stateStorage); err != nil {
		return errors.Wrap(err, "couldn't close output pull engine")
	}

	return nil
}
