package streaming

import (
	"context"
	"log"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql/streaming/storage"

	"github.com/cube2222/octosql/execution"
)

// Based on protocol buffer max timestamp value.
var maxWatermark = time.Date(9999, 1, 1, 1, 1, 1, 1, time.UTC)

type WatermarkSource interface {
	GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error)
}

type IntermediateRecordStore interface {
	AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, record *execution.Record) error
	Next(ctx context.Context, tx storage.StateTransaction) (*execution.Record, error)
	UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error
	GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error)
	MarkEndOfStream(ctx context.Context, tx storage.StateTransaction) error
	Close() error
}

type PullEngine struct {
	irs                    IntermediateRecordStore
	source                 execution.RecordStream
	lastCommittedWatermark time.Time
	watermarkSource        WatermarkSource
	storage                *storage.BadgerStorage
}

func NewPullEngine(irs IntermediateRecordStore, storage *storage.BadgerStorage, source execution.RecordStream, watermarkSource WatermarkSource) *PullEngine {
	return &PullEngine{
		irs:             irs,
		storage:         storage,
		source:          source,
		watermarkSource: watermarkSource,
	}
}

func (engine *PullEngine) Run(ctx context.Context) {
	for {
		err := engine.loop(ctx)
		if err == execution.ErrEndOfStream {
			log.Println("end of stream, stopping loop")
			return
		}
		if err != nil {
			log.Println(err)
		}
	}
}

func (engine *PullEngine) loop(ctx context.Context) error {
	tx := engine.storage.BeginTransaction()
	defer tx.Abort()
	watermark, err := engine.watermarkSource.GetWatermark(ctx, tx)
	if err != nil {
		return errors.Wrap(err, "couldn't get current watermark from source")
	}
	if watermark.After(engine.lastCommittedWatermark) {
		err := engine.irs.UpdateWatermark(ctx, tx, watermark)
		if err != nil {
			return errors.Wrap(err, "couldn't update watermark in intermediate record store")
		}
		err = tx.Commit()
		if err != nil {
			return errors.Wrap(err, "couldn't commit transaction")
		}
		engine.lastCommittedWatermark = watermark
		return nil
	}

	record, err := engine.source.Next(storage.InjectStateTransaction(ctx, tx))
	if err != nil {
		if err == execution.ErrEndOfStream {
			err := engine.irs.UpdateWatermark(ctx, tx, maxWatermark)
			if err != nil {
				return errors.Wrap(err, "couldn't mark end of stream max watermark in intermediate record store")
			}
			err = engine.irs.MarkEndOfStream(ctx, tx)
			if err != nil {
				return errors.Wrap(err, "couldn't mark end of stream in intermediate record store")
			}
			err = tx.Commit()
			if err != nil {
				return errors.Wrap(err, "couldn't commit transaction")
			}
			return execution.ErrEndOfStream
		}
		return errors.Wrap(err, "couldn't get next record")
	}
	err = engine.irs.AddRecord(ctx, tx, 0, record)
	if err != nil {
		return errors.Wrap(err, "couldn't add record to intermediate record store")
	}
	err = tx.Commit()
	if err != nil {
		return errors.Wrap(err, "couldn't commit transaction")
	}

	return nil
}

func (engine *PullEngine) Next(ctx context.Context) (*execution.Record, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	rec, err := engine.irs.Next(ctx, tx)
	if err != nil {
		if err == execution.ErrEndOfStream {
			return nil, execution.ErrEndOfStream
		}
		return nil, errors.Wrap(err, "couldn't get next record from intermediate record store")
	}
	return rec, nil
}

func (engine *PullEngine) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	return engine.irs.GetWatermark(ctx, tx)
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
