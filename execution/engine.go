package execution

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql/storage"
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
var MaxWatermark = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)

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
	TriggerKeys(ctx context.Context, tx storage.StateTransaction, batchSize int) (int, error)
	MarkEndOfStream(ctx context.Context, tx storage.StateTransaction) error
	MarkError(ctx context.Context, tx storage.StateTransaction, err error) error
	Close(ctx context.Context, storage storage.Storage) error
}

type PullEngine struct {
	irs                    IntermediateRecordStore
	sources                []RecordStream
	lastCommittedWatermark time.Time
	watermarkSource        WatermarkSource
	storage                storage.Storage
	streamID               *StreamID
	batchSizeManager       *BatchSizeManager
	shouldPrefixStreamID   bool

	ctx          context.Context
	ctxCancel    func()
	closeErrChan chan error
}

func NewPullEngine(irs IntermediateRecordStore, storage storage.Storage, sources []RecordStream, streamID *StreamID, watermarkSource WatermarkSource, shouldPrefixStreamID bool, ctx context.Context) *PullEngine {
	ctx, cancel := context.WithCancel(ctx)

	return &PullEngine{
		irs:                  irs,
		storage:              storage,
		sources:              sources,
		streamID:             streamID,
		watermarkSource:      watermarkSource,
		batchSizeManager:     NewBatchSizeManager(time.Second / 4),
		shouldPrefixStreamID: shouldPrefixStreamID,
		ctx:                  ctx,
		ctxCancel:            cancel,
		closeErrChan:         make(chan error),
	}
}

func (engine *PullEngine) getPrefixedTx(tx storage.StateTransaction) storage.StateTransaction {
	if engine.shouldPrefixStreamID {
		return tx.WithPrefix(engine.streamID.AsPrefix())
	}
	return tx
}

func (engine *PullEngine) Run() {
	tx := engine.storage.BeginTransaction()
	endOfStreamReached := false

	for {
		select {
		case <-engine.ctx.Done():
			engine.closeErrChan <- engine.ctx.Err()
			return
		default:
		}

		if engine.batchSizeManager.RecordsLeftToTake() == 0 {
			// Refresh transaction takes care of committing and creating new transactions.
			tx = engine.refreshTransaction(tx)
		}

		triggeredCount, err := engine.irs.TriggerKeys(engine.ctx, engine.getPrefixedTx(tx), engine.batchSizeManager.RecordsLeftToTake())
		if triggeredCount == 0 && err == nil {
			// Nothing triggered, just go forth, if the stream hasn't ended yet.
			if endOfStreamReached {
				if err := tx.Commit(); err != nil {
					log.Println("engine: couldn't commit: ", err)
					continue
				}
				log.Println("engine: end of stream, all keys triggered, stopping loop")
				engine.closeErrChan <- nil
				return
			}
		} else if triggeredCount > 0 {
			// We triggered possibly lot of records, it can be time to commit now.
			engine.batchSizeManager.MarkRecordsProcessed(triggeredCount)
			tx = engine.refreshTransaction(tx)

			continue
		} else if err != nil {
			triggerErr := err
			log.Println("couldn't trigger keys: ", err)
			tx.Abort()
			tx = engine.storage.BeginTransaction()
			err := engine.irs.MarkError(engine.ctx, engine.getPrefixedTx(tx), triggerErr)
			if err != nil {
				log.Fatalf("couldn't mark error on intermediate record store: %s", err)
			}
			if err := tx.Commit(); err != nil {
				log.Fatalf("couldn't commit marking error on intermediate record store: %s", err)
			}

			engine.closeErrChan <- triggerErr
			return
		}

		err = engine.loop(engine.ctx, tx)
		if err == ErrEndOfStream {
			err := tx.Commit()
			if err == nil {
				endOfStreamReached = true
			} else {
				log.Println("engine: couldn't commit: ", err)
			}

			tx = engine.storage.BeginTransaction()
			continue
		} else if errors.Cause(err) == ErrNewTransactionRequired {
			err := tx.Commit()
			if err != nil {
				log.Println("engine: couldn't commit: ", err)
			}
			engine.batchSizeManager.CommitSuccessful()
			tx = engine.storage.BeginTransaction()
		} else if waitableError := GetErrWaitForChanges(err); waitableError != nil {
			err := tx.Commit()
			if err != nil {
				log.Println("engine: couldn't commit: ", err)
			}
			engine.batchSizeManager.CommitSuccessful()
			err = waitableError.ListenForChanges(engine.ctx)
			if err != nil {
				log.Println("engine: couldn't listen for changes: ", err)
			}
			err = waitableError.Close()
			if err != nil {
				log.Println("engine: couldn't close listening for changes: ", err)
			}
			engine.batchSizeManager.Reset()
			tx = engine.storage.BeginTransaction()
		} else if err != nil {
			loopErr := err
			log.Println("engine: ", loopErr)
			tx.Abort()
			tx = engine.storage.BeginTransaction()
			err := engine.irs.MarkError(engine.ctx, engine.getPrefixedTx(tx), loopErr)
			if err != nil {
				log.Fatalf("couldn't mark error on intermediate record store: %s", err)
			}
			if err := tx.Commit(); err != nil {
				log.Fatalf("couldn't commit marking error on intermediate record store: %s", err)
			}

			engine.closeErrChan <- loopErr
			return
		} else if err == nil {
			engine.batchSizeManager.MarkRecordsProcessed(1)
			tx = engine.refreshTransaction(tx)
		}
	}
}

func (engine *PullEngine) refreshTransaction(tx storage.StateTransaction) storage.StateTransaction {
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
	return tx
}

func (engine *PullEngine) loop(ctx context.Context, tx storage.StateTransaction) error {
	// This is a transaction prefixed with the current node StreamID,
	// which should be used for all storage operations of this node.
	// Source streams will get the raw, non-prefixed, transaction.
	prefixedTx := engine.getPrefixedTx(tx)

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

	// We want to read from sources in some random order, so that we don't always read from the first one
	sourceOrder := rand.Perm(len(engine.sources))

	var record *Record
	var inputIndex int
	areAllEndOfStream := true

	for _, sourceIndex := range sourceOrder {
		record, err = engine.sources[sourceIndex].Next(storage.InjectStateTransaction(ctx, tx))
		if err != nil {
			if err == ErrEndOfStream {
				continue
			}

			return errors.Wrap(err, "couldn't get next record")
		}

		areAllEndOfStream = false
		inputIndex = sourceIndex
		break
	}

	if areAllEndOfStream {
		err := engine.irs.UpdateWatermark(ctx, prefixedTx, MaxWatermark)
		if err != nil {
			return errors.Wrap(err, "couldn't mark end of stream max watermark in intermediate record store")
		}

		err = engine.irs.MarkEndOfStream(ctx, prefixedTx)
		if err != nil {
			return errors.Wrap(err, "couldn't mark end of stream in intermediate record store")
		}
		return ErrEndOfStream
	}

	err = engine.irs.AddRecord(ctx, prefixedTx, inputIndex, record)
	if err != nil {
		return errors.Wrap(err, "couldn't add record to intermediate record store")
	}

	return nil
}

func (engine *PullEngine) Next(ctx context.Context) (*Record, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	prefixedTx := engine.getPrefixedTx(tx)

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
	prefixedTx := engine.getPrefixedTx(tx)

	return engine.irs.GetWatermark(ctx, prefixedTx)
}

func (engine *PullEngine) Close(ctx context.Context, storage storage.Storage) error {
	engine.ctxCancel()
	err := <-engine.closeErrChan
	if err == context.Canceled || err == context.DeadlineExceeded {
	} else if err != nil {
		return errors.Wrap(err, "couldn't stop pull engine")
	}

	for i := range engine.sources {
		if err := engine.sources[i].Close(ctx, storage); err != nil {
			return errors.Wrapf(err, "couldn't close source stream with index %v", i)
		}
	}

	if err := engine.irs.Close(ctx, storage); err != nil {
		return errors.Wrap(err, "couldn't close intermediate record store")
	}

	if err := storage.DropAll(engine.streamID.AsPrefix()); err != nil {
		return errors.Wrap(err, "couldn't clear storage with streamID prefix")
	}

	return nil
}
