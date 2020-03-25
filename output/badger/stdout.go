package badger

import (
	"bytes"
	"context"
	"sort"
	"time"

	tm "github.com/buger/goterm"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/output/table"
	"github.com/cube2222/octosql/streaming/storage"
)

type RecordsLister interface {
	ListRecords(ctx context.Context, tx storage.StateTransaction) ([]*execution.Record, error)
}

type StdOutPrinter struct {
	recordsLister RecordsLister
	stateStorage  storage.Storage
}

func NewStdOutPrinter(stateStorage storage.Storage, recordsLister RecordsLister) *StdOutPrinter {
	return &StdOutPrinter{
		recordsLister: recordsLister,
		stateStorage:  stateStorage,
	}
}

func (printer *StdOutPrinter) Run(ctx context.Context) error {
	for range time.Tick(time.Second / 10) {
		tx := printer.stateStorage.BeginTransaction()

		records, err := printer.recordsLister.ListRecords(ctx, tx)
		if err != nil {
			return errors.Wrap(err, "couldn't list records")
		}
		if len(records) > 0 && !records[0].EventTimeField().Empty() {
			sort.SliceStable(records, func(i, j int) bool {
				return records[i].EventTime().AsTime().Before(records[j].EventTime().AsTime())
			})
		}

		tm.Clear()

		var buf bytes.Buffer
		tabWriter := table.NewOutput(&buf, false)
		for _, rec := range records {
			if err := tabWriter.WriteRecord(rec); err != nil {
				return errors.Wrap(err, "couldn't write record to table")
			}
		}
		if err := tabWriter.Close(); err != nil {
			return errors.Wrap(err, "couldn't close table writer")
		}

		if _, err := tm.Printf(buf.String()); err != nil {
			return errors.Wrap(err, "couldn't printf output")
		}
		tm.Flush()

		tx.Abort()
	}
	return nil
}
