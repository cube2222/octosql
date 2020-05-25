package integration

import (
	"context"
	"io/ioutil"
	"log"
	"reflect"
	"runtime"

	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/datasources/csv"
	"github.com/cube2222/octosql/datasources/excel"
	"github.com/cube2222/octosql/datasources/json"
	"github.com/cube2222/octosql/datasources/kafka"
	"github.com/cube2222/octosql/datasources/parquet"
	"github.com/cube2222/octosql/datasources/redis"
	"github.com/cube2222/octosql/datasources/sql/mysql"
	"github.com/cube2222/octosql/datasources/sql/postgres"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/parser"
	"github.com/cube2222/octosql/parser/sqlparser"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/physical/optimizer"
	"github.com/cube2222/octosql/storage"
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"github.com/pkg/errors"
)

func MainCopy(query, configPath string) (execution.RecordStream, error) {
	ctx := context.Background()
	cfg, err := config.ReadConfig(configPath)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't read config")
	}

	// Setup the datasource repo
	dataSourceRepository, err := physical.CreateDataSourceRepositoryFromConfig(
		map[string]physical.Factory{
			"csv":      csv.NewDataSourceBuilderFactoryFromConfig,
			"json":     json.NewDataSourceBuilderFactoryFromConfig,
			"mysql":    mysql.NewDataSourceBuilderFactoryFromConfig,
			"postgres": postgres.NewDataSourceBuilderFactoryFromConfig,
			"redis":    redis.NewDataSourceBuilderFactoryFromConfig,
			"excel":    excel.NewDataSourceBuilderFactoryFromConfig,
			"kafka":    kafka.NewDataSourceBuilderFactoryFromConfig,
			"parquet":  parquet.NewDataSourceBuilderFactoryFromConfig,
		},
		cfg,
	)
	if err != nil {
		log.Fatal(err)
	}

	// Parse query
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse query")
	}

	typed, ok := stmt.(sqlparser.SelectStatement)
	if !ok {
		return nil, errors.Errorf("invalid statement type, wanted sqlparser.SelectStatement, got %v", reflect.TypeOf(stmt))
	}

	plan, err := parser.ParseNode(typed)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse statement")
	}

	// Get badger storage
	storageDirectory, err := ioutil.TempDir("", "octosql")
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create temporary directory")
	}

	opts := badger.DefaultOptions(storageDirectory)
	if runtime.GOOS == "windows" {
		opts = opts.WithValueLogLoadingMode(options.FileIO)
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't open in-memory badger database")
	}

	stateStorage := storage.NewBadgerStorage(db)

	// Transform to physical
	sourceNodes, variables, err := plan.Physical(ctx, logical.NewPhysicalPlanCreator(dataSourceRepository, cfg.Physical))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create physical plan")
	}

	// We only want one partition at the end, to print the output easily.
	shuffled := physical.NewShuffle(1, physical.NewConstantStrategy(0), sourceNodes)

	// Only the first partition is there.
	var phys physical.Node = shuffled[0]
	phys = optimizer.Optimize(ctx, optimizer.DefaultScenarios, phys)

	exec, err := phys.Materialize(ctx, physical.NewMaterializationContext(cfg, stateStorage))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize the physical plan into an execution plan")
	}

	stream, _, err := execution.GetAndStartAllShuffles(ctx, stateStorage, execution.NewStreamID("root"), []execution.Node{exec}, variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get record stream from execution plan")
	}
	/*
		outputSinkFn := func(stateStorage storage.Storage, streamID *execution.StreamID, eventTimeField octosql.VariableName) (store execution.IntermediateRecordStore, printer output.Printer) {
			sink := batch.NewTableOutput(streamID, eventTimeField)
			outputPrinter := batch.NewLiveTablePrinter(stateStorage, sink, batchtable.TableFormatter(false))
			return sink, outputPrinter
		}

		outStreamID := &execution.StreamID{Id: "output"}

		outputSink, printer := outputSinkFn(stateStorage, outStreamID, phys.Metadata().EventTimeField())

		pullEngine := execution.NewPullEngine(outputSink, stateStorage, []execution.RecordStream{stream[0]}, outStreamID, execOutput[0].WatermarkSource, false, ctx)
		go pullEngine.Run()

		if err := printer.Run(ctx); err != nil {
			return nil, errors.Wrap(err, "couldn't run stdout printer")
		}

		if err := pullEngine.Close(ctx, stateStorage); err != nil {
			return nil, errors.Wrap(err, "couldn't close output pull engine")
		}

	*/
	// In the end we have only one stream
	return stream[0], nil
}
