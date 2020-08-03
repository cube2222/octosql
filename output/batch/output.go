package batch

import (
	"context"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/storage"
)

type TableOutput struct {
	StreamID            *execution.StreamID
	EventTimeField      octosql.VariableName
	OrderingExpressions []execution.Expression
	OrderingDirections  []execution.OrderDirection
	Limit               *int
	Offset              *int
}

func NewTableOutput(
	streamID *execution.StreamID,
	eventTimeField octosql.VariableName,
	orderingExpressions []execution.Expression,
	orderingDirections []execution.OrderDirection,
	limit *int,
	offset *int,
) *TableOutput {
	return &TableOutput{
		StreamID:            streamID,
		EventTimeField:      eventTimeField,
		OrderingExpressions: orderingExpressions,
		OrderingDirections:  orderingDirections,
		Limit:               limit,
		Offset:              offset,
	}
}

var recordsPrefix = []byte("$records$")
var watermarkPrefix = []byte("$watermark$")
var errorPrefix = []byte("$error$")
var endOfStreamPrefix = []byte("$end_of_stream$")

func (o *TableOutput) ReadyForMore(ctx context.Context, tx storage.StateTransaction) error {
	return nil
}

func (o *TableOutput) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, record *execution.Record) error {
	tx = tx.WithPrefix(o.StreamID.AsPrefix())
	if inputIndex != 0 {
		return errors.Errorf("only one input stream allowed for output, got input index %d", inputIndex)
	}
	records := storage.NewMap(tx.WithPrefix(recordsPrefix))

	variables := record.AsVariables()

	orderingPrefix := []byte("$")
	for i := range o.OrderingExpressions {
		expressionValue, err := o.OrderingExpressions[i].ExpressionValue(ctx, variables)
		if err != nil {
			return errors.Wrapf(err, "couldn't evaluate expression with index %d", i)
		}
		if o.OrderingDirections[i] == execution.Ascending {
			orderingPrefix = append(orderingPrefix, expressionValue.MonotonicMarshal()...)
		} else {
			orderingPrefix = append(orderingPrefix, expressionValue.ReversedMonotonicMarshal()...)
		}
		orderingPrefix = append(orderingPrefix, '$')
	}

	recordKV := map[string]octosql.Value{}
	for _, field := range record.Fields() {
		recordKV[field.Name.String()] = record.Value(field.Name)
	}

	objectKey := octosql.MakeObject(recordKV)

	key := execution.NewOrderByKey(append(orderingPrefix, objectKey.MonotonicMarshal()...))

	var recordData RecordData
	err := records.Get(key, &recordData)
	if err == storage.ErrNotFound {
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current IDs for record value")
	}

	if len(recordData.Ids) == 0 {
		recordData.Ids = append(recordData.Ids, record.ID())
		recordData.IsUndo = record.IsUndo()
		recordData.Record = record
	} else if recordData.IsUndo != record.IsUndo() {
		recordData.Ids = recordData.Ids[:len(recordData.Ids)-1]
	} else {
		recordData.Ids = append(recordData.Ids, record.ID())
	}

	if len(recordData.Ids) == 0 {
		if err := records.Delete(key); err != nil {
			return errors.Wrap(err, "couldn't delete record from output records")
		}
	} else {
		if err := records.Set(key, &recordData); err != nil {
			return errors.Wrap(err, "couldn't remove single record ID from output records")
		}
	}

	return nil
}

func (o *TableOutput) Next(ctx context.Context, tx storage.StateTransaction) (*execution.Record, error) {
	panic("no next in output")
}

func (o *TableOutput) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	tx = tx.WithPrefix(o.StreamID.AsPrefix())
	watermarkState := storage.NewValueState(tx.WithPrefix(watermarkPrefix))

	octoWatermark := octosql.MakeTime(watermark)
	if err := watermarkState.Set(&octoWatermark); err != nil {
		return errors.Wrap(err, "couldn't save new watermark value")
	}

	return nil
}

func (o *TableOutput) TriggerKeys(ctx context.Context, tx storage.StateTransaction, batchSize int) (int, error) {
	return 0, nil
}

func (o *TableOutput) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	tx = tx.WithPrefix(o.StreamID.AsPrefix())
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

func (o *TableOutput) MarkEndOfStream(ctx context.Context, tx storage.StateTransaction) error {
	tx = tx.WithPrefix(o.StreamID.AsPrefix())
	endOfStreamState := storage.NewValueState(tx.WithPrefix(endOfStreamPrefix))

	phantom := octosql.MakePhantom()
	if err := endOfStreamState.Set(&phantom); err != nil {
		return errors.Wrap(err, "couldn't mark end of stream")
	}

	return nil
}

func (o *TableOutput) GetEndOfStream(ctx context.Context, tx storage.StateTransaction) (bool, error) {
	tx = tx.WithPrefix(o.StreamID.AsPrefix())
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

func (o *TableOutput) MarkError(ctx context.Context, tx storage.StateTransaction, err error) error {
	tx = tx.WithPrefix(o.StreamID.AsPrefix())
	errorState := storage.NewValueState(tx.WithPrefix(errorPrefix))

	octoError := octosql.MakeString(err.Error())
	if err := errorState.Set(&octoError); err != nil {
		return errors.Wrap(err, "couldn't mark error")
	}

	return nil
}

func (o *TableOutput) GetErrorMessage(ctx context.Context, tx storage.StateTransaction) (string, error) {
	tx = tx.WithPrefix(o.StreamID.AsPrefix())
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

func (o *TableOutput) Close(ctx context.Context, storage storage.Storage) error {
	if err := storage.DropAll(o.StreamID.AsPrefix()); err != nil {
		return errors.Wrap(err, "couldn't clear storage with streamID prefix")
	}

	return nil
}

func (o *TableOutput) ListRecords(ctx context.Context, tx storage.StateTransaction) ([]*execution.Record, error) {
	tx = tx.WithPrefix(o.StreamID.AsPrefix())
	records := storage.NewMap(tx.WithPrefix(recordsPrefix))

	iter := records.GetIterator()

	offsetCounter, limitCounter := 0, 0

	var outRecords []*execution.Record
	var err error
	var octoKey execution.OrderByKey
	var recordData RecordData
	for err = iter.Next(&octoKey, &recordData); err == nil; err = iter.Next(&octoKey, &recordData) {
		offsetCounter++
		if o.Offset != nil && offsetCounter <= *o.Offset {
			continue
		}
		if o.Limit != nil && limitCounter == *o.Limit {
			break
		}
		limitCounter++

		var fields []string
		for _, k := range recordData.Record.Fields() {
			fields = append(fields, k.Name.String())
		}
		sort.Strings(fields)

		data := make([]octosql.Value, len(fields))
		for i := range fields {
			data[i] = recordData.Record.Value(octosql.NewVariableName(fields[i]))
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
	if err != nil && err != storage.ErrEndOfIterator {
		return nil, errors.Wrap(err, "couldn't iterate over records")
	}
	if err := iter.Close(); err != nil {
		return nil, errors.Wrap(err, "couldn't close iterator")
	}

	return outRecords, nil
}
