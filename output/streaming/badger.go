package streaming

import (
	"bytes"
	"context"
	"sort"
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

	if !o.EventTimeField.Empty() {
		if err := o.Trigger.RecordReceived(ctx, tx.WithPrefix(triggerPrefix), record.EventTime(), record.EventTime().AsTime()); err != nil {
			return errors.Wrap(err, "couldn't mark record received in watermark trigger")
		}

		tx := tx.WithPrefix(bytes.Join([][]byte{[]byte("$"), record.EventTime().MonotonicMarshal(), []byte("$")}, nil))

		if !record.IsUndo() {
			records := storage.NewDeque(tx.WithPrefix(recordsPrefix))
			if err := records.PushBack(record); err != nil {
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
	}

	return nil
}

func (o *StreamOutput) Next(ctx context.Context, tx storage.StateTransaction) (*execution.Record, error) {
	panic("no next in output")
}

func (o *StreamOutput) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	watermarkState := storage.NewValueState(tx.WithPrefix(watermarkPrefix))

	octoWatermark := octosql.MakeTime(watermark)
	if err := watermarkState.Set(&octoWatermark); err != nil {
		return errors.Wrap(err, "couldn't save new watermark value")
	}

	return nil
}

func (o *StreamOutput) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	watermarkState := storage.NewValueState(tx.WithPrefix(watermarkPrefix))

	var octoWatermark octosql.Value
	err := watermarkState.Get(&octoWatermark)
	if err == storage.ErrNotFound {
		return time.Time{}, nil
	} else if err != nil {
		return time.Time{}, errors.Wrap(err, "couldn't get current watermark value")
	}

	return octoWatermark.AsTime(), nil
}

func (o *StreamOutput) MarkEndOfStream(ctx context.Context, tx storage.StateTransaction) error {
	endOfStreamState := storage.NewValueState(tx.WithPrefix(endOfStreamPrefix))

	phantom := octosql.MakePhantom()
	if err := endOfStreamState.Set(&phantom); err != nil {
		return errors.Wrap(err, "couldn't mark end of stream")
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
	endOfStreamState := storage.NewValueState(tx.WithPrefix(errorPrefix))

	octoError := octosql.MakeString(err.Error())
	if err := endOfStreamState.Set(&octoError); err != nil {
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

func (o *StreamOutput) ListRecords(ctx context.Context, tx storage.StateTransaction) ([]*execution.Record, error) {
	records := storage.NewMap(tx.WithPrefix(recordsPrefix))

	iter := records.GetIterator()

	var outRecords []*execution.Record
	var err error
	var octoKey octosql.Value
	var recordData RecordData
	for err = iter.Next(&octoKey, &recordData); err == nil; err = iter.Next(&octoKey, &recordData) {
		object := octoKey.AsMap()
		var fields []string
		for k := range object {
			fields = append(fields, k)
		}
		sort.Strings(fields)

		data := make([]octosql.Value, len(fields))
		for i := range fields {
			data[i] = object[fields[i]]
		}

		variableNames := make([]octosql.VariableName, len(fields))
		for i := range fields {
			variableNames[i] = octosql.NewVariableName(fields[i])
		}

		for _, id := range recordData.Ids {
			opts := []execution.RecordOption{
				execution.WithID(id),
				execution.WithEventTimeField(o.EventTimeField),
			}
			if recordData.IsUndo {
				opts = append(opts, execution.WithUndo())
			}

			outRecords = append(
				outRecords,
				execution.NewRecordFromSlice(
					variableNames,
					data,
					opts...,
				),
			)
		}
	}
	if err != storage.ErrEndOfIterator {
		return nil, errors.Wrap(err, "couldn't iterate over records")
	}
	if err := iter.Close(); err != nil {
		return nil, errors.Wrap(err, "couldn't close iterator")
	}

	return outRecords, nil
}
