package badger

import (
	"bytes"
	"context"
	"io"
	"os"
	"sort"
	"time"

	tm "github.com/buger/goterm"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/storage"
)

type TableFormatter func(w io.Writer, records []*execution.Record, watermark time.Time, err error) error

type RecordsLister interface {
	ListRecords(ctx context.Context, tx storage.StateTransaction) ([]*execution.Record, error)
	GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error)
	GetEndOfStream(ctx context.Context, tx storage.StateTransaction) (bool, error)
	GetErrorMessage(ctx context.Context, tx storage.StateTransaction) (string, error)
}

type LiveTablePrinter struct {
	recordsLister  RecordsLister
	stateStorage   storage.Storage
	tableFormatter TableFormatter
}

func NewLiveTablePrinter(stateStorage storage.Storage, recordsLister RecordsLister, tableFormatter TableFormatter) *LiveTablePrinter {
	return &LiveTablePrinter{
		recordsLister:  recordsLister,
		stateStorage:   stateStorage,
		tableFormatter: tableFormatter,
	}
}

func (printer *LiveTablePrinter) Run(ctx context.Context) error {
	for range time.Tick(time.Second / 4) {
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

		endOfStream, err := printer.recordsLister.GetEndOfStream(ctx, tx)
		if err != nil {
			return errors.Wrap(err, "couldn't check if end of stream has been reached")
		}

		watermark, err := printer.recordsLister.GetWatermark(ctx, tx)
		if err != nil {
			return errors.Wrap(err, "couldn't get current watermark")
		}

		errorMessage, err := printer.recordsLister.GetErrorMessage(ctx, tx)
		if err != nil {
			return errors.Wrap(err, "couldn't check if there was an error")
		}

		var errorToPrint error
		if len(errorMessage) > 0 {
			errorToPrint = errors.New(errorMessage)
		}

		var buf bytes.Buffer
		if err := printer.tableFormatter(&buf, records, watermark, errorToPrint); err != nil {
			return errors.Wrap(err, "couldn't format table")
		}

		tm.Clear()
		if _, err := tm.Printf(buf.String()); err != nil {
			return errors.Wrap(err, "couldn't printf output")
		}
		tm.Flush()

		tx.Abort()

		if len(errorMessage) > 0 {
			return errors.New(errorMessage)
		} else if endOfStream {
			_, err := buf.WriteTo(os.Stdout)
			return err
		}
	}
	panic("unreachable")
}

type WholeTablePrinter struct {
	recordsLister  RecordsLister
	stateStorage   storage.Storage
	tableFormatter TableFormatter
}

func NewWholeTablePrinter(stateStorage storage.Storage, recordsLister RecordsLister, tableFormatter TableFormatter) *WholeTablePrinter {
	return &WholeTablePrinter{
		recordsLister:  recordsLister,
		stateStorage:   stateStorage,
		tableFormatter: tableFormatter,
	}
}

func (printer *WholeTablePrinter) Run(ctx context.Context) error {
	for range time.Tick(time.Second / 4) {
		tx := printer.stateStorage.BeginTransaction()

		endOfStream, err := printer.recordsLister.GetEndOfStream(ctx, tx)
		if err != nil {
			return errors.Wrap(err, "couldn't check if end of stream has been reached")
		}

		errorMessage, err := printer.recordsLister.GetErrorMessage(ctx, tx)
		if err != nil {
			return errors.Wrap(err, "couldn't check if there was an error")
		}

		if !endOfStream && len(errorMessage) == 0 {
			continue
		}

		records, err := printer.recordsLister.ListRecords(ctx, tx)
		if err != nil {
			return errors.Wrap(err, "couldn't list records")
		}

		if len(records) > 0 && !records[0].EventTimeField().Empty() {
			sort.SliceStable(records, func(i, j int) bool {
				return records[i].EventTime().AsTime().Before(records[j].EventTime().AsTime())
			})
		}

		watermark, err := printer.recordsLister.GetWatermark(ctx, tx)
		if err != nil {
			return errors.Wrap(err, "couldn't get current watermark")
		}

		var errorToPrint error
		if len(errorMessage) > 0 {
			errorToPrint = errors.New(errorMessage)
		}

		var buf bytes.Buffer
		if err := printer.tableFormatter(&buf, records, watermark, errorToPrint); err != nil {
			return errors.Wrap(err, "couldn't format table")
		}

		if _, err := buf.WriteTo(os.Stdout); err != nil {
			return errors.Wrap(err, "couldn't print output")
		}

		tx.Abort()

		if len(errorMessage) > 0 {
			return errors.New(errorMessage)
		} else if endOfStream {
			return nil
		}
	}
	panic("unreachable")
}
