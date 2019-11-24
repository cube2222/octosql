package streaming

import (
	"log"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql/execution"
)

type RecordSink interface {
	AddRecord(record *execution.Record, ack func()) error
	MarkEndOfStream() error
}

type PullEngine struct {
	recordSink      RecordSink
	source          execution.RecordStream
	ackFunc         func(id execution.ID)
	watermarkSource func() (time.Time, bool)
}

func NewPullEngine(sink RecordSink) *PullEngine {
	return &PullEngine{
		recordSink: NewBuffer(),
	}
}

func (engine *PullEngine) Run() {
	for {
		err := engine.loop()
		if err == execution.ErrEndOfStream {
			log.Println("end of stream, stopping loop")
			return
		}
		if err != nil {
			log.Println(err)
		}
	}
}

func (engine *PullEngine) loop() error {
	r, err := engine.source.Next()
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
	err = engine.recordSink.AddRecord(r, func() {
		engine.ackFunc(r.ID())
	})
	if err != nil {
		return errors.Wrap(err, "couldn't add record to sink")
	}

	return nil
}
