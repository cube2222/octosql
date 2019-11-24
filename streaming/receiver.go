package streaming

import (
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql/execution"
)

type Receiver struct {
	recordSink      RecordSink
	ackFunc         func(id execution.ID)
	watermarkSource func() (time.Time, bool)
}

func (r *Receiver) Receive(record *execution.Record) error {
	err := r.recordSink.AddRecord(record, func() {
		r.ackFunc(record.ID())
	})
	if err != nil {
		return errors.Wrap(err, "couldn't add record to sink")
	}

	return nil
}

func (b *Receiver) MarkEndOfStream() error {
	return b.recordSink.MarkEndOfStream()
}
