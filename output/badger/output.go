package badger

import (
	"context"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/streaming/storage"
)

type Output struct {
	EventTimeField octosql.VariableName
}

var recordsPrefix = []byte("$records$")
var watermarkPrefix = []byte("$watermark$")
var errorPrefix = []byte("$error$")
var endOfStreamPrefix = []byte("$end_of_stream$")

func (o *Output) ReadyForMore(ctx context.Context, tx storage.StateTransaction) error {
	return nil
}

func (o *Output) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, record *execution.Record) error {
	if inputIndex != 0 {
		return errors.Errorf("only one input stream allowed for output, got input index %d", inputIndex)
	}
	records := storage.NewMap(tx.WithPrefix(recordsPrefix))

	recordKV := map[string]octosql.Value{}
	for _, field := range record.Fields() {
		recordKV[field.Name.String()] = record.Value(field.Name)
	}

	key := octosql.MakeObject(recordKV)

	var recordData RecordData
	err := records.Get(&key, &recordData)
	if err == storage.ErrNotFound {
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current IDs for record value")
	}

	if len(recordData.Ids) == 0 {
		recordData.Ids = append(recordData.Ids, record.ID())
		recordData.IsUndo = record.IsUndo()
	} else if recordData.IsUndo != record.IsUndo() {
		recordData.Ids = recordData.Ids[:len(recordData.Ids)-1]
	} else {
		recordData.Ids = append(recordData.Ids, record.ID())
	}

	if len(recordData.Ids) == 0 {
		if err := records.Delete(&key); err != nil {
			return errors.Wrap(err, "couldn't delete record from output records")
		}
	} else {
		if err := records.Set(&key, &recordData); err != nil {
			return errors.Wrap(err, "couldn't remove single record ID from output records")
		}
	}

	return nil
}

func (o *Output) Next(ctx context.Context, tx storage.StateTransaction) (*execution.Record, error) {
	panic("no next in output")
}

func (o *Output) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	watermarkState := storage.NewValueState(tx.WithPrefix(watermarkPrefix))

	octoWatermark := octosql.MakeTime(watermark)
	if err := watermarkState.Set(&octoWatermark); err != nil {
		return errors.Wrap(err, "couldn't save new watermark value")
	}

	return nil
}

func (o *Output) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
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

func (o *Output) MarkEndOfStream(ctx context.Context, tx storage.StateTransaction) error {
	endOfStreamState := storage.NewValueState(tx.WithPrefix(endOfStreamPrefix))

	phantom := octosql.MakePhantom()
	if err := endOfStreamState.Set(&phantom); err != nil {
		return errors.Wrap(err, "couldn't mark end of stream")
	}

	return nil
}

func (o *Output) GetEndOfStream(ctx context.Context, tx storage.StateTransaction) (bool, error) {
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

func (o *Output) MarkError(ctx context.Context, tx storage.StateTransaction, err error) error {
	endOfStreamState := storage.NewValueState(tx.WithPrefix(errorPrefix))

	octoError := octosql.MakeString(err.Error())
	if err := endOfStreamState.Set(&octoError); err != nil {
		return errors.Wrap(err, "couldn't mark error")
	}

	return nil
}

func (o *Output) GetErrorMessage(ctx context.Context, tx storage.StateTransaction) (string, error) {
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

func (o *Output) Close() error {
	return nil // TODO: Cleanup?
}

func (o *Output) ListRecords(ctx context.Context, tx storage.StateTransaction) ([]*execution.Record, error) {
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
