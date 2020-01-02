package streaming

import (
	"context"
	"log"

	"github.com/cube2222/octosql/streaming/storage"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql/execution"
)

type RecordSink interface {
	AddRecord(ctx context.Context, record *execution.Record, tx storage.StateTransaction)
	MarkEndOfStream(ctx context.Context) error
}

type PullEngine struct {
	recordSink RecordSink
	source     execution.RecordStream
	storage    *storage.BadgerStorage
}

func NewPullEngine(sink RecordSink, storage *storage.BadgerStorage, source execution.RecordStream) *PullEngine {
	return &PullEngine{
		recordSink: sink,
		storage:    storage,
		source:     source,
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
	// TODO: Have trigger manager here with tx.WithPrefix(triggerManager) passed

	tx := engine.storage.BeginTransaction()
	r, err := engine.source.Next(storage.InjectStateTransaction(ctx, tx))
	if err != nil {
		if err == execution.ErrEndOfStream {
			err := engine.recordSink.MarkEndOfStream(ctx)
			if err != nil {
				return errors.Wrap(err, "couldn't mark end of stream")
			}
			return execution.ErrEndOfStream
		}
		return errors.Wrap(err, "couldn't get next record")
	}
	engine.recordSink.AddRecord(ctx, r, tx)

	return nil
}
