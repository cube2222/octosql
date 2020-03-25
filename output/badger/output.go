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
var errorPrefix = []byte("$error$")
var endOfStreamPrefix = []byte("$end_of_stream$")

func (o *Output) ReadyForMore(ctx context.Context, tx storage.StateTransaction) error {
	return nil
}

func (o *Output) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, record *execution.Record) error {
	if inputIndex != 0 {
		return errors.Errorf("input with index other than 0 not allowed in output, got %d", inputIndex)
	}
	records := storage.NewMap(tx.WithPrefix(recordsPrefix))

	recordKV := map[string]octosql.Value{}
	for _, field := range record.Fields() {
		recordKV[field.Name.String()] = record.Value(field.Name)
	}

	key := octosql.MakeObject(recordKV)

	var octoIDs octosql.Value
	err := records.Get(&key, &octoIDs)
	if err == storage.ErrNotFound {
		octoIDs = octosql.MakeTuple(nil)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current IDs for record value")
	}
	IDs := octoIDs.AsSlice()

	if !record.IsUndo() {
		newIDs := octosql.MakeTuple(append(IDs, octosql.MakeString(record.ID().Show())))
		if err := records.Set(&key, &newIDs); err != nil {
			return errors.Wrap(err, "couldn't add record ID to output records")
		}
	} else {
		IDs = IDs[:len(IDs)-1]
		if len(IDs) == 0 {
			if err := records.Delete(&key); err != nil {
				return errors.Wrap(err, "couldn't delete record from output records")
			}
		} else {
			newIDs := octosql.MakeTuple(IDs)
			if err := records.Set(&key, &newIDs); err != nil {
				return errors.Wrap(err, "couldn't remove single record ID from output records")
			}
		}
	}

	return nil
}

func (o *Output) Next(ctx context.Context, tx storage.StateTransaction) (*execution.Record, error) {
	panic("no next in output")
}

func (o *Output) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	return nil
}

func (o *Output) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	panic("implement me")
}

func (o *Output) MarkEndOfStream(ctx context.Context, tx storage.StateTransaction) error {
	return nil
}

func (o *Output) MarkError(ctx context.Context, tx storage.StateTransaction, err error) error {
	return nil
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
	var octoIDs octosql.Value
	for err = iter.Next(&octoKey, &octoIDs); err == nil; err = iter.Next(&octoKey, &octoIDs) {
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

		for _, id := range octoIDs.AsSlice() {
			outRecords = append(
				outRecords,
				execution.NewRecordFromSlice(
					variableNames,
					data,
					execution.WithID(execution.NewRecordID(id.AsString())),
					execution.WithEventTimeField(o.EventTimeField),
				),
			)
		}
	}
	if err := iter.Close(); err != nil {
		return nil, errors.Wrap(err, "couldn't close iterator")
	}
	if err != storage.ErrEndOfIterator {
		return nil, errors.Wrap(err, "couldn't iterate over records")
	}

	return outRecords, nil
}
