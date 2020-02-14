package execution

import (
	"context"
	"fmt"
	"log"
	"time"

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

type ZeroWatermarkSource struct {
}

func (z *ZeroWatermarkSource) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	return time.Time{}, nil
}

type IntermediateRecordStore interface {
	AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, record *Record) error
	Next(ctx context.Context, tx storage.StateTransaction) (*Record, error)
	UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error
	GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error)
	MarkEndOfStream(ctx context.Context, tx storage.StateTransaction) error
	Close() error
}

type PullEngine struct {
	irs                    IntermediateRecordStore
	source                 RecordStream
	lastCommittedWatermark time.Time
	watermarkSource        WatermarkSource
	storage                storage.Storage
	nodeStatePrefix        []byte
}

func NewPullEngine(irs IntermediateRecordStore, storage storage.Storage, source RecordStream, nodeStatePrefix []byte, watermarkSource WatermarkSource) *PullEngine {
	return &PullEngine{
		irs:             irs,
		storage:         storage,
		source:          source,
		nodeStatePrefix: nodeStatePrefix,
		watermarkSource: watermarkSource,
	}
}

func (engine *PullEngine) Run(ctx context.Context) {
	tx := engine.storage.BeginTransaction()

	i := 0
	for {
		i++
		if i%1 == 0 {
			err := tx.Commit()
			if err != nil {
				log.Println("couldn't commit: ", err)
				return
			}
			tx = engine.storage.BeginTransaction()
		}
		err := engine.loop(ctx, tx)
		if err == ErrEndOfStream {
			err := tx.Commit()
			if err != nil {
				log.Println("couldn't commit: ", err)
			}
			log.Println("end of stream, stopping loop")
			return
		}
		if errors.Cause(err) == ErrNewTransactionRequired {
			log.Println("engine new transaction required")
			err := tx.Commit()
			if err != nil {
				log.Println("couldn't commit: ", err)
				return
			}
			tx = engine.storage.BeginTransaction()
		} else if waitableError := GetErrWaitForChanges(err); waitableError != nil {
			err := tx.Commit()
			if err != nil {
				log.Println("couldn't commit: ", err)
				return
			}
			log.Println("engine listening for changes")
			err = waitableError.ListenForChanges(ctx)
			if err != nil {
				log.Println("couldn't listen for changes: ", err)
			}
			err = waitableError.Close()
			if err != nil {
				log.Println("couldn't close listening for changes: ", err)
			}
			tx = engine.storage.BeginTransaction()
		} else if err != nil {
			tx.Abort()
			log.Println(err)
			return // TODO: Error propagation? Add this to the underlying queue as an ErrorElement? How to do this well?
		}
	}
}

func (engine *PullEngine) loop(ctx context.Context, tx storage.StateTransaction) error {
	prefixedTx := tx.WithPrefix(engine.nodeStatePrefix)

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
	prefixedTx := tx.WithPrefix(engine.nodeStatePrefix)

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
	prefixedTx := tx.WithPrefix(engine.nodeStatePrefix)

	return engine.irs.GetWatermark(ctx, prefixedTx)
}

func (engine *PullEngine) Close() error {
	err := engine.source.Close()
	if err != nil {
		return errors.Wrap(err, "couldn't close source stream")
	}

	err = engine.irs.Close()
	if err != nil {
		return errors.Wrap(err, "couldn't close intermediate record store")
	}

	return nil
}
