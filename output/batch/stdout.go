package batch

import (
	"bytes"
	"context"
	"os"
	"sort"
	"time"

	"github.com/gosuri/uilive"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/storage"
)

const REFRESH_DELAY = time.Second / 4

type RecordsLister interface {
	ListRecords(ctx context.Context, tx storage.StateTransaction) ([]*execution.Record, error)
	GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error)
	GetEndOfStream(ctx context.Context, tx storage.StateTransaction) (bool, error)
	GetErrorMessage(ctx context.Context, tx storage.StateTransaction) (string, error)
}

type LiveTablePrinter struct {
	stateStorage   storage.Storage
	recordsLister  RecordsLister
	tableFormatter TableFormatter
}

func NewLiveTablePrinter(stateStorage storage.Storage, recordsLister RecordsLister, tableFormatter TableFormatter) *LiveTablePrinter {
	return &LiveTablePrinter{
		stateStorage:   stateStorage,
		recordsLister:  recordsLister,
		tableFormatter: tableFormatter,
	}
}

func (printer *LiveTablePrinter) Run(ctx context.Context) error {
	liveWriter := uilive.New()

	for range time.Tick(REFRESH_DELAY) {
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

		buf.WriteTo(liveWriter)
		liveWriter.Flush()

		tx.Abort()

		if errorToPrint != nil {
			return errorToPrint
		} else if endOfStream {
			return nil
		}
	}
	panic("unreachable")
}

type BatchTablePrinter struct {
	stateStorage   storage.Storage
	recordsLister  RecordsLister
	tableFormatter TableFormatter
}

func NewWholeTablePrinter(stateStorage storage.Storage, recordsLister RecordsLister, tableFormatter TableFormatter) *BatchTablePrinter {
	return &BatchTablePrinter{
		stateStorage:   stateStorage,
		recordsLister:  recordsLister,
		tableFormatter: tableFormatter,
	}
}

func (printer *BatchTablePrinter) Run(ctx context.Context) error {
	for range time.Tick(REFRESH_DELAY) {
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

		if errorToPrint != nil {
			return errorToPrint
		} else if endOfStream {
			return nil
		}
	}
	panic("unreachable")
}
