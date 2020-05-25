package integration

import (
	"context"
	"io/ioutil"
	"log"
	"reflect"
	"runtime"
	"time"

	"github.com/cube2222/octosql"
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

	stream, execOutput, err := execution.GetAndStartAllShuffles(ctx, stateStorage, execution.NewStreamID("root"), []execution.Node{exec}, variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get record stream from execution plan")
	}

	outStreamID := execution.NewStreamID("$all_records$")
	srs := NewSetRecordStore(outStreamID)

	pullEngine := execution.NewPullEngine(srs, stateStorage, []execution.RecordStream{stream[0]}, outStreamID, execOutput[0].WatermarkSource, false, ctx)
	go pullEngine.Run()

	records, err := srs.getRecords(ctx, stateStorage)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get records")
	}

	if err := pullEngine.Close(ctx, stateStorage); err != nil {
		return nil, errors.Wrap(err, "couldn't close pull engine")
	}

	for i := range records {
		println(records[i].Show())
	}

	tx := stateStorage.BeginTransaction()
	rs := execution.NewInMemoryStream(storage.InjectStateTransaction(ctx, tx), records)
	if err := tx.Commit(); err != nil {
		return nil, errors.Wrap(err, "couldn't commit transaction to create in memory stream")
	}

	return rs, nil
}

// This is an IRS that just stores the records sent to it
type SetRecordStore struct {
	streamID     *execution.StreamID
	streamPrefix []byte
}

func NewSetRecordStore(streamID *execution.StreamID) *SetRecordStore {
	return &SetRecordStore{
		streamID:     streamID,
		streamPrefix: []byte(streamID.Id),
	}
}

var recordsPrefix = []byte("$records$")
var watermarkPrefix = []byte("$watermark$")
var errorPrefix = []byte("$error$")
var endOfStreamPrefix = []byte("$end_of_stream$")

func (srs *SetRecordStore) ReadyForMore(ctx context.Context, tx storage.StateTransaction) error {
	return nil
}

func (srs *SetRecordStore) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, record *execution.Record) error {
	tx = tx.WithPrefix(srs.streamPrefix).WithPrefix(recordsPrefix)
	//fmt.Printf("srs.AddRecord() tx prefix: %s\n", tx.Prefix())
	records := storage.NewMultiSet(tx)
	value := execution.RecordToValue(record, true, true, true)

	if err := records.Insert(value); err != nil {
		return errors.Wrap(err, "couldn't insert record into set")
	}
	return nil
}

func (srs *SetRecordStore) ReadRecords(tx storage.StateTransaction) ([]*execution.Record, error) {
	tx = tx.WithPrefix(srs.streamPrefix).WithPrefix(recordsPrefix)
	// fmt.Printf("srs.ReadRecords() tx prefix = %s\n", tx.Prefix())
	recordSet := storage.NewMultiSet(tx)

	values, err := recordSet.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, "couldn't read records from set")
	}

	records := make([]*execution.Record, len(values))
	for i := range values {
		records[i] = execution.ValueToRecord(values[i])
	}

	return records, nil
}

func (srs *SetRecordStore) getRecords(ctx context.Context, stateStorage storage.Storage) ([]*execution.Record, error) {
	for range time.Tick(time.Second) {
		tx := stateStorage.BeginTransaction()
		println("Checking for end of stream...")
		isEOS, err := srs.GetEndOfStream(ctx, tx)
		if errors.Cause(err) == execution.ErrNewTransactionRequired {
		} else if err != nil {
			return nil, errors.Wrap(err, "couldn't check for end of stream")
		}

		if isEOS {
			break
		}
	}

	return srs.ReadRecords(stateStorage.BeginTransaction())
}

func (srs *SetRecordStore) Next(ctx context.Context, tx storage.StateTransaction) (*execution.Record, error) {
	panic("no next for SetRecordStore")
}

func (srs *SetRecordStore) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	watermarkState := storage.NewValueState(tx.WithPrefix(srs.streamPrefix).WithPrefix(watermarkPrefix))

	octoWatermark := octosql.MakeTime(watermark)
	if err := watermarkState.Set(&octoWatermark); err != nil {
		return errors.Wrap(err, "couldn't save new watermark value")
	}

	return nil
}

func (srs *SetRecordStore) TriggerKeys(ctx context.Context, tx storage.StateTransaction, batchSize int) (int, error) {
	return 0, nil
}

func (srs *SetRecordStore) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	watermarkState := storage.NewValueState(tx.WithPrefix(srs.streamPrefix).WithPrefix(watermarkPrefix))

	var octoWatermark octosql.Value
	err := watermarkState.Get(&octoWatermark)
	if err == storage.ErrNotFound {
		return time.Time{}, nil
	} else if err != nil {
		return time.Time{}, errors.Wrap(err, "couldn't get current watermark value")
	}

	return octoWatermark.AsTime(), nil
}

func (srs *SetRecordStore) MarkEndOfStream(ctx context.Context, tx storage.StateTransaction) error {
	endOfStreamState := storage.NewValueState(tx.WithPrefix(srs.streamPrefix).WithPrefix(endOfStreamPrefix))

	phantom := octosql.MakePhantom()
	if err := endOfStreamState.Set(&phantom); err != nil {
		return errors.Wrap(err, "couldn't mark end of stream")
	}

	return nil
}

func (srs *SetRecordStore) GetEndOfStream(ctx context.Context, tx storage.StateTransaction) (bool, error) {
	endOfStreamState := storage.NewValueState(tx.WithPrefix(srs.streamPrefix).WithPrefix(endOfStreamPrefix))

	var octoEndOfStream octosql.Value
	err := endOfStreamState.Get(&octoEndOfStream)
	if err == storage.ErrNotFound {
		return false, nil
	} else if err != nil {
		return false, errors.Wrap(err, "couldn't get end of stream value")
	}

	return true, nil
}

func (srs *SetRecordStore) MarkError(ctx context.Context, tx storage.StateTransaction, err error) error {
	errorState := storage.NewValueState(tx.WithPrefix(srs.streamPrefix).WithPrefix(errorPrefix))

	octoError := octosql.MakeString(err.Error())
	if err := errorState.Set(&octoError); err != nil {
		return errors.Wrap(err, "couldn't mark error")
	}

	return nil
}

func (srs *SetRecordStore) Close(ctx context.Context, storage storage.Storage) error {
	if err := storage.DropAll(srs.streamPrefix); err != nil {
		return errors.Wrap(err, "couldn't clear storage with streamID prefix")
	}

	return nil
}
