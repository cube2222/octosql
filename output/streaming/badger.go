package streaming

import (
	"bytes"
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/trigger"
	"github.com/cube2222/octosql/streaming/storage"
)

type StreamOutput struct {
	EventTimeField octosql.VariableName
	Trigger        trigger.WatermarkTrigger
}

var recordsPrefix = []byte("$records$")
var retractionsPrefix = []byte("$retractions$")
var triggerPrefix = []byte("$trigger$")
var watermarkPrefix = []byte("$watermark$")
var errorPrefix = []byte("$error$")
var endOfStreamPrefix = []byte("$end_of_stream$")

func (o *StreamOutput) ReadyForMore(ctx context.Context, tx storage.StateTransaction) error {
	return nil
}

func (o *StreamOutput) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, record *execution.Record) error {
	if inputIndex != 0 {
		return errors.Errorf("only one input stream allowed for output, got input index %d", inputIndex)
	}

	var eventTime octosql.Value
	if !o.EventTimeField.Empty() {
		eventTime = record.EventTime()
	} else {
		eventTime = octosql.MakeTime(time.Time{})
	}

	if err := o.Trigger.RecordReceived(ctx, tx.WithPrefix(triggerPrefix), eventTime, eventTime.AsTime()); err != nil {
		return errors.Wrap(err, "couldn't mark record received in watermark trigger")
	}

	tx = tx.WithPrefix(bytes.Join([][]byte{[]byte("$"), record.EventTime().MonotonicMarshal(), []byte("$")}, nil))

	if !record.IsUndo() {
		records := execution.NewOutputQueue(tx.WithPrefix(recordsPrefix))
		if err := records.Push(ctx, record); err != nil {
			return errors.Wrap(err, "couldn't add record to records queue")
		}
	} else {
		recordKV := map[string]octosql.Value{}
		for _, field := range record.Fields() {
			recordKV[field.Name.String()] = record.Value(field.Name)
		}
		key := octosql.MakeObject(recordKV)

		var curCount octosql.Value
		retractions := storage.NewMap(tx.WithPrefix(retractionsPrefix))
		err := retractions.Get(&key, &curCount)
		if err == storage.ErrNotFound {
			curCount = octosql.MakeInt(0)
		} else if err != nil {
			return errors.Wrap(err, "couldn't get current record count from retractions map")
		}

		curCount = octosql.MakeInt(curCount.AsInt() + 1)
		if err := retractions.Set(&key, &curCount); err != nil {
			return errors.Wrap(err, "couldn't add record to retractions map")
		}
	}

	return nil
}

func (o *StreamOutput) Next(ctx context.Context, tx storage.StateTransaction) (*execution.Record, error) {
	// If no element to get and end of stream then end of stream else wait. Also check error.
	records := execution.NewOutputQueue(tx.WithPrefix(recordsPrefix))
	var record execution.Record
	err := records.Pop(ctx, &record)
	if execution.GetErrWaitForChanges(err) != nil {
		errWaitForChanges := err

		var value octosql.Value

		endOfStreamState := storage.NewValueState(tx.WithPrefix(endOfStreamPrefix))
		err := endOfStreamState.Get(&value)
		if err == nil {
			return nil, execution.ErrEndOfStream
		} else if err == storage.ErrNotFound {
		} else if err != nil {
			return nil, errors.Wrap(err, "couldn't check end of stream state")
		}

		errorState := storage.NewValueState(tx.WithPrefix(errorPrefix))
		err = errorState.Get(&value)
		if err == nil {
			return nil, errors.New(value.AsString())
		} else if err == storage.ErrNotFound {
		} else if err != nil {
			return nil, errors.Wrap(err, "couldn't check end of stream state")
		}

		return nil, errWaitForChanges
	} else if err != nil {
		return nil, errors.Wrap(err, "couldn't get record from records queue")
	}
	return &record, nil
}

func (o *StreamOutput) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	if err := o.Trigger.UpdateWatermark(ctx, tx.WithPrefix(triggerPrefix), execution.MaxWatermark); err != nil {
		return errors.Wrap(err, "couldn't update watermark in trigger")
	}
	// Trigger
	return nil
}

func (o *StreamOutput) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	panic("not implemented")
}

func (o *StreamOutput) MarkEndOfStream(ctx context.Context, tx storage.StateTransaction) error {
	endOfStreamState := storage.NewValueState(tx.WithPrefix(endOfStreamPrefix))

	phantom := octosql.MakePhantom()
	if err := endOfStreamState.Set(&phantom); err != nil {
		return errors.Wrap(err, "couldn't mark end of stream")
	}

	if err := o.Trigger.UpdateWatermark(ctx, tx.WithPrefix(triggerPrefix), execution.MaxWatermark); err != nil {
		return errors.Wrap(err, "couldn't update watermark in trigger to max watermark")
	}

	return nil
}

func (o *StreamOutput) GetEndOfStream(ctx context.Context, tx storage.StateTransaction) (bool, error) {
	endOfStreamState := storage.NewValueState(tx.WithPrefix(endOfStreamPrefix))

	var octoEndOfStream octosql.Value
	err := endOfStreamState.Get(&octoEndOfStream)
	if err == storage.ErrNotFound {
		return false, nil
	} else if err != nil {
		return false, errors.Wrap(err, "couldn't get end of stream value")
	}

	return true, nil
}

func (o *StreamOutput) MarkError(ctx context.Context, tx storage.StateTransaction, err error) error {
	errorState := storage.NewValueState(tx.WithPrefix(errorPrefix))

	octoError := octosql.MakeString(err.Error())
	if err := errorState.Set(&octoError); err != nil {
		return errors.Wrap(err, "couldn't mark error")
	}

	return nil
}

func (o *StreamOutput) GetErrorMessage(ctx context.Context, tx storage.StateTransaction) (string, error) {
	errorState := storage.NewValueState(tx.WithPrefix(errorPrefix))

	var octoError octosql.Value
	err := errorState.Get(&octoError)
	if err == storage.ErrNotFound {
		return "", nil
	} else if err != nil {
		return "", errors.Wrap(err, "couldn't get error message")
	}

	return octoError.AsString(), nil
}

func (o *StreamOutput) Close() error {
	return nil // TODO: Cleanup?
}
