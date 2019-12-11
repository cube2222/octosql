package streaming

import (
	"context"
	"log"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql/execution"
)

type RecordSink interface {
	AddRecord(ctx context.Context, record *execution.Record, tx StateTransaction)
	MarkEndOfStream(ctx context.Context) error
}

type PullEngine struct {
	recordSink RecordSink
	source     execution.RecordStream
	storage    *BadgerStorage
}

func NewPullEngine(sink RecordSink, storage *BadgerStorage, source execution.RecordStream) *PullEngine {
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
	r, err := engine.source.Next(InjectStateTransaction(ctx, tx))
	if err != nil {
		if err == execution.ErrEndOfStream {
			err := engine.recordSink.MarkEndOfStream()
			if err != nil {
				return errors.Wrap(err, "couldn't mark end of stream")
			}
			return execution.ErrEndOfStream
		}
		return errors.Wrap(err, "couldn't get next record")
	}
	engine.recordSink.AddRecord(r, tx)

	return nil
}
