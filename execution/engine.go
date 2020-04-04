package execution

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql/streaming/storage"
)

var ErrNewTransactionRequired = fmt.Errorf("new transaction required")

type ErrWaitForChanges struct {
	*storage.Subscription
}

func NewErrWaitForChanges(subscription *storage.Subscription) error {
	return &ErrWaitForChanges{Subscription: subscription}
}

func (e *ErrWaitForChanges) Error() string {
	return "wait for changes"
}

func GetErrWaitForChanges(err error) *ErrWaitForChanges {
	if err == nil {
		return nil
	}
	if err, ok := errors.Cause(err).(*ErrWaitForChanges); ok {
		return err
	}
	return nil
}

// Based on protocol buffer max timestamp value.
var maxWatermark = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)

type WatermarkSource interface {
	GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error)
}

type IntermediateRecordStore interface {
	// ReadyForMore is used to check if the intermediate record store is able to consume more data.
	// This allows it to communicate back-pressure.
	ReadyForMore(ctx context.Context, tx storage.StateTransaction) error
	AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, record *Record) error
	Next(ctx context.Context, tx storage.StateTransaction) (*Record, error)
	UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error
	GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error)
	MarkEndOfStream(ctx context.Context, tx storage.StateTransaction) error
	MarkError(ctx context.Context, tx storage.StateTransaction, err error) error
	Close() error
}

type PullEngine struct {
	irs                    IntermediateRecordStore
	source                 RecordStream
	lastCommittedWatermark time.Time
	watermarkSource        WatermarkSource
	storage                storage.Storage
	streamID               *StreamID
	batchSizeManager       *BatchSizeManager
}

func NewPullEngine(irs IntermediateRecordStore, storage storage.Storage, source RecordStream, streamID *StreamID, watermarkSource WatermarkSource) *PullEngine {
	return &PullEngine{
		irs:              irs,
		storage:          storage,
		source:           source,
		streamID:         streamID,
		watermarkSource:  watermarkSource,
		batchSizeManager: NewBatchSizeManager(time.Second),
	}
}

func (engine *PullEngine) Run(ctx context.Context) {
	tx := engine.storage.BeginTransaction()

	for {
		err := engine.loop(ctx, tx)
		if err == ErrEndOfStream {
			err := tx.Commit()
			if err != nil {
				log.Println("engine: couldn't commit: ", err)
			}
			log.Println("engine: end of stream, stopping loop")
			return
		} else if errors.Cause(err) == ErrNewTransactionRequired {
			log.Println("engine: new transaction required")
			err := tx.Commit()
			if err != nil {
				log.Println("engine: couldn't commit: ", err)
			}
			engine.batchSizeManager.CommitSuccessful()
			tx = engine.storage.BeginTransaction()
		} else if waitableError := GetErrWaitForChanges(err); waitableError != nil {
			log.Println("engine: listening for changes")
			err := tx.Commit()
			if err != nil {
				log.Println("engine: couldn't commit: ", err)
			}
			engine.batchSizeManager.CommitSuccessful()
			err = waitableError.ListenForChanges(ctx)
			if err != nil {
				log.Println("engine: couldn't listen for changes: ", err)
			}
			log.Println("engine: received change")
			err = waitableError.Close()
			if err != nil {
				log.Println("engine: couldn't close listening for changes: ", err)
			}
			engine.batchSizeManager.Reset()
			tx = engine.storage.BeginTransaction()
		} else if err != nil {
			tx.Abort()
			log.Println("engine: ", err)
			tx = engine.storage.BeginTransaction()
			err := engine.irs.MarkError(ctx, tx.WithPrefix(engine.streamID.AsPrefix()), err)
			if err != nil {
				log.Fatalf("couldn't mark error on intermediate record store: %s", err)
			}
			if err := tx.Commit(); err != nil {
				log.Fatalf("couldn't commit marking error on intermediate record store: %s", err)
			}
			return
		}

		if !engine.batchSizeManager.ShouldTakeNextRecord() {
			err := tx.Commit()
			if err != nil {
				if errors.Cause(err) == badger.ErrTxnTooBig {
					engine.batchSizeManager.CommitTooBig()
				}
				log.Println("engine: couldn't commit: ", err)
			} else {
				engine.batchSizeManager.CommitSuccessful()
			}
			tx = engine.storage.BeginTransaction()
		}
	}
}

func (engine *PullEngine) loop(ctx context.Context, tx storage.StateTransaction) error {
	// This is a transaction prefixed with the current node StreamID,
	// which should be used for all storage operations of this node.
	// Source streams will get the raw, non-prefixed, transaction.
	prefixedTx := tx.WithPrefix(engine.streamID.AsPrefix())

	watermark, err := engine.watermarkSource.GetWatermark(ctx, tx)
	if err != nil {
		return errors.Wrap(err, "couldn't get current watermark from source")
	}
	if watermark.After(engine.lastCommittedWatermark) {
		err := engine.irs.UpdateWatermark(ctx, prefixedTx, watermark)
		if err != nil {
			return errors.Wrap(err, "couldn't update watermark in intermediate record store")
		}
		engine.lastCommittedWatermark = watermark // TODO: last _commited_ watermark :( this is not committed
		return nil
	}

	if err := engine.irs.ReadyForMore(ctx, prefixedTx); err != nil {
		return errors.Wrap(err, "couldn't check if intermediate record store can take more records")
	}

	record, err := engine.source.Next(storage.InjectStateTransaction(ctx, tx))
	if err != nil {
		if err == ErrEndOfStream {
			err := engine.irs.UpdateWatermark(ctx, prefixedTx, maxWatermark)
			if err != nil {
				return errors.Wrap(err, "couldn't mark end of stream max watermark in intermediate record store")
			}
			err = engine.irs.MarkEndOfStream(ctx, prefixedTx)
			if err != nil {
				return errors.Wrap(err, "couldn't mark end of stream in intermediate record store")
			}
			return ErrEndOfStream
		}
		return errors.Wrap(err, "couldn't get next record")
	}
	err = engine.irs.AddRecord(ctx, prefixedTx, 0, record)
	if err != nil {
		return errors.Wrap(err, "couldn't add record to intermediate record store")
	}

	return nil
}

func (engine *PullEngine) Next(ctx context.Context) (*Record, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	prefixedTx := tx.WithPrefix(engine.streamID.AsPrefix())

	rec, err := engine.irs.Next(ctx, prefixedTx)
	if err != nil {
		if err == ErrEndOfStream {
			return nil, ErrEndOfStream
		}
		return nil, errors.Wrap(err, "couldn't get next record from intermediate record store")
	}
	return rec, nil
}

func (engine *PullEngine) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	prefixedTx := tx.WithPrefix(engine.streamID.AsPrefix())

	return engine.irs.GetWatermark(ctx, prefixedTx)
}

func (engine *PullEngine) Close(ctx context.Context) error {
	if err := engine.source.Close(ctx); err != nil {
		return errors.Wrap(err, "couldn't close source stream")
	}

	if err := engine.irs.Close(); err != nil {
		return errors.Wrap(err, "couldn't close intermediate record store")
	}

	return nil
}
