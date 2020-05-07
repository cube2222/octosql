package streaming

import (
	"context"
	"log"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/storage"
)

type StreamPrinter struct {
	stateStorage storage.Storage
	recordSink   execution.IntermediateRecordStore
	printFn      func(record *execution.Record)
}

func NewStreamPrinter(stateStorage storage.Storage, recordSink execution.IntermediateRecordStore, printFn func(record *execution.Record)) *StreamPrinter {
	return &StreamPrinter{
		stateStorage: stateStorage,
		recordSink:   recordSink,
		printFn:      printFn,
	}
}

func (sp *StreamPrinter) Run(ctx context.Context) error {
	for {
		tx := sp.stateStorage.BeginTransaction()
		ctx := storage.InjectStateTransaction(ctx, tx)

		rec, err := sp.recordSink.Next(ctx, tx)
		if err == execution.ErrEndOfStream {
			err := tx.Commit()
			if err != nil {
				log.Println("couldn't commit transaction: ", err)
				continue
			}
			break
		} else if errors.Cause(err) == execution.ErrNewTransactionRequired {
			if err := tx.Commit(); err != nil {
				log.Println("couldn't commit transaction: ", err)
			}
			continue
		} else if waitableError := execution.GetErrWaitForChanges(err); waitableError != nil {
			err := tx.Commit()
			if err != nil {
				log.Println("couldn't commit transaction: ", err)
				continue
			}
			err = waitableError.ListenForChanges(ctx)
			if err != nil {
				log.Println("couldn't listen for changes: ", err)
			}
			err = waitableError.Close()
			if err != nil {
				log.Println("couldn't close subscription: ", err)
			}
			continue
		} else if err != nil {
			return errors.Wrap(err, "couldn't get next record")
		}

		sp.printFn(rec)

		err = tx.Commit()
		if err != nil {
			log.Println("error committing output print transaction (this can lead to duplicate output records): ", err)
			continue
		}
	}
	return nil
}
