package execution

import (
	"context"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

type ProcessFunction interface {
	AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, key octosql.Value, record *Record) error
	Trigger(ctx context.Context, tx storage.StateTransaction, key octosql.Value) ([]*Record, error) // New Records and Retractions
}

type Trigger interface {
	RecordReceived(ctx context.Context, tx storage.StateTransaction, key octosql.Value, eventTime time.Time) error
	UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error
	PollKeysToFire(ctx context.Context, tx storage.StateTransaction, batchSize int) ([]octosql.Value, error)
	KeysFired(ctx context.Context, tx storage.StateTransaction, key []octosql.Value) error
}

type ProcessByKey struct {
	trigger         Trigger
	eventTimeField  octosql.VariableName // Empty if not grouping by event time.
	keyExpressions  [][]Expression
	processFunction ProcessFunction
	variables       octosql.Variables

	garbageCollectorCtxCancel    func()
	garbageCollectorCloseErrChan chan error
}

var eventTimesSeenPrefix = []byte("event_times_seen") // used to keep track of every event time seen and to collect garbage

func (p *ProcessByKey) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, record *Record) error {
	recordVariables := record.AsVariables()
	variables, err := p.variables.MergeWith(recordVariables)
	if err != nil {
		return errors.Wrap(err, "couldn't merge stream variables with record")
	}

	keyExpressions := p.keyExpressions[inputIndex]

	key := make([]octosql.Value, len(keyExpressions))
	for i := range keyExpressions {
		if _, ok := keyExpressions[i].(*RecordExpression); ok {
			key[i], err = keyExpressions[i].ExpressionValue(ctx, recordVariables)
		} else {
			key[i], err = keyExpressions[i].ExpressionValue(ctx, variables)
		}
		if err != nil {
			return errors.Wrapf(err, "couldn't evaluate process key expression with index %v", i)
		}
	}

	keyTuple := octosql.MakeTuple(key)

	eventTime := MaxWatermark
	if len(p.eventTimeField) > 0 {
		eventTime = record.EventTime().AsTime()

		// Adding keyTuple to event time storage
		eventTimeMap := storage.NewMap(tx.WithPrefix(eventTimesSeenPrefix))

		octoEventTime := octosql.MakeTime(eventTime)
		var keysRecordsTuple octosql.Value
		err := eventTimeMap.Get(&octoEventTime, &keysRecordsTuple)
		if err == storage.ErrNotFound {
			keysRecordsTuple = octosql.ZeroTuple()
		} else if err != nil {
			return errors.Wrap(err, "couldn't get records event time tuple from storage")
		}

		// We need to store both keyTuple (to perform KeysFired in trigger) and record (to perform retraction in process function)
		// 0: keyTuple, 1: record, 2: inputIndex
		keyRecordTuple := octosql.MakeTuple([]octosql.Value{keyTuple, record, octosql.MakeInt(inputIndex)})

		keysRecordsTuple = octosql.MakeTuple(append(keysRecordsTuple.AsSlice(), keyRecordTuple))
		if err := eventTimeMap.Set(&octoEventTime, &keysRecordsTuple); err != nil {
			return errors.Wrap(err, "couldn't add records event time tuple to storage")
		}
	}

	// Here garbage collector will just drop event time prefixes
	err = p.processFunction.AddRecord(ctx, tx, inputIndex, keyTuple, record)
	if err != nil {
		return errors.Wrap(err, "couldn't add record to process function")
	}

	// Here garbage collector will call KeysFired so that trigger itself can remove records from storage
	err = p.trigger.RecordReceived(ctx, tx, keyTuple, eventTime)
	if err != nil {
		return errors.Wrap(err, "couldn't mark record received in trigger")
	}

	return nil
}

func (p *ProcessByKey) RunGarbageCollector(ctx context.Context, prefixedStorage storage.Storage) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		tx := prefixedStorage.BeginTransaction()

		// Check current output watermark value
		watermark, err := p.GetWatermark(ctx, tx)
		if err != nil {
			return errors.Wrap(err, "couldn't get output watermark value")
		}

		// We don't want to clear whole storage when sending MaxWatermark - on EndOfStream and WatermarkTrigger
		if watermark != MaxWatermark {
			// Collect every event time earlier than watermark - 10min
			boundary := watermark.Add(-10 * time.Minute) // TODO - make this configurable

			eventTimeMap := storage.NewMap(tx.WithPrefix(eventTimesSeenPrefix))
			octoEventTimeSlice := make([]octosql.Value, 0)

			var key, value octosql.Value
			it := eventTimeMap.GetIterator()
			for err := it.Next(&key, &value); err != storage.ErrEndOfIterator; err = it.Next(&key, &value) {
				eventTime := key.AsTime()

				if eventTime.Before(boundary) {
					octoEventTimeSlice = append(octoEventTimeSlice, key)

					keyTuples := make([]octosql.Value, 0)

					keysRecordsSlice := value.AsSlice()
					for _, keyRecord := range keysRecordsSlice {
						keyTuple := keyRecord.AsSlice()[0]
						keyTuples = append(keyTuples, keyTuple)

						recordTuple := keyRecord.AsSlice()[1]
						inputIndex := keyRecord.AsSlice()[2]

						// Retract every "old enough" record from process function
						if err = p.processFunction.AddRecord(ctx, tx, inputIndex.AsInt(), keyTuple, recordTuple); err != nil {
							return errors.Wrap(err, "couldn't retract old enough record from process function")
						}
					}

					// Mark every "old enough" key as fired -> trigger will delete this key itself
					if err = p.trigger.KeysFired(ctx, tx, keyTuples); err != nil {
						return errors.Wrap(err, "couldn't mark old enough keys as fired in trigger")
					}
				} else {
					break
				}
			}
			if err := it.Close(); err != nil {
				return errors.Wrap(err, "couldn't close event time map iterator")
			}

			// Drop every "old enough" event time from event time map
			for _, octoEventTime := range octoEventTimeSlice {
				if err := eventTimeMap.Delete(&octoEventTime); err != nil {
					return errors.Wrapf(err, "couldn't delete event time %v from event time map", octoEventTime.AsTime())
				}
			}
		}

		time.Sleep(10 * time.Millisecond) // TODO - this is very poor in testing, make this configurable?
	}
}

var outputWatermarkPrefix = []byte("$output_watermark$")
var pendingWatermarkPrefix = []byte("$pending_watermark$")
var endOfStreamPrefix = []byte("$end_of_stream$")
var pendingEndOfStreamPrefix = []byte("$pending_end_of_stream$")
var outputQueuePrefix = []byte("$output_queue$")

func (p *ProcessByKey) Next(ctx context.Context, tx storage.StateTransaction) (*Record, error) {
	outputQueue := NewOutputQueue(tx.WithPrefix(outputQueuePrefix))

	endOfStreamState := storage.NewValueState(tx.WithPrefix(endOfStreamPrefix))
	var eos octosql.Value
	err := endOfStreamState.Get(&eos)
	if err == storage.ErrNotFound {
	} else if err != nil {
		return nil, errors.Wrap(err, "couldn't get end of stream value")
	} else {
		return nil, ErrEndOfStream
	}

	var element QueueElement
	for err = outputQueue.Pop(ctx, &element); err == nil; err = outputQueue.Pop(ctx, &element) {
		switch payload := element.Type.(type) {
		case *QueueElement_Record:
			return payload.Record, nil
		case *QueueElement_Watermark:
			outputWatermarkState := storage.NewValueState(tx.WithPrefix(outputWatermarkPrefix))
			watermark, err := ptypes.Timestamp(payload.Watermark)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't parse watermark timestamp")
			}
			octoWatermark := octosql.MakeTime(watermark)
			err = outputWatermarkState.Set(&octoWatermark)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't update output watermark")
			}
		case *QueueElement_EndOfStream:
			octoEndOfStream := octosql.MakeBool(true)
			err := endOfStreamState.Set(&octoEndOfStream)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't update end of stream state")
			}
			return nil, ErrEndOfStream
		case *QueueElement_Error:
			return nil, errors.New(payload.Error)
		default:
			panic("invalid queue element type")
		}
	}

	return nil, errors.Wrap(err, "couldn't pop element from output queue")
}

func (p *ProcessByKey) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	err := p.trigger.UpdateWatermark(ctx, tx, watermark)
	if err != nil {
		return errors.Wrap(err, "couldn't update watermark in trigger")
	}

	t, err := ptypes.TimestampProto(watermark)
	if err != nil {
		return errors.Wrap(err, "couldn't convert time to proto timestamp")
	}
	pendingWatermarkState := storage.NewValueState(tx.WithPrefix(pendingWatermarkPrefix))
	if err := pendingWatermarkState.Set(t); err != nil {
		return errors.Wrap(err, "couldn't set pending watermark state")
	}

	return nil
}

func (p *ProcessByKey) TriggerKeys(ctx context.Context, tx storage.StateTransaction, batchSize int) (int, error) {
	outputQueue := NewOutputQueue(tx.WithPrefix(outputQueuePrefix))

	keys, err := p.trigger.PollKeysToFire(ctx, tx, batchSize)
	if err != nil {
		return 0, errors.Wrap(err, "couldn't poll keys to fire")
	}

	if len(keys) == 0 {
		// Send any pending watermark
		pendingWatermarkState := storage.NewValueState(tx.WithPrefix(pendingWatermarkPrefix))

		var t timestamp.Timestamp
		err := pendingWatermarkState.Get(&t)
		if err == storage.ErrNotFound {
		} else if err != nil {
			return 0, errors.Wrap(err, "couldn't get pending watermark state")
		} else {

			if err := outputQueue.Push(ctx, &QueueElement{
				Type: &QueueElement_Watermark{
					Watermark: &t,
				},
			}); err != nil {
				return 0, errors.Wrap(err, "couldn't push watermark to output queue")
			}

			if err := pendingWatermarkState.Clear(); err != nil {
				return 0, errors.Wrap(err, "couldn't clear pending watermark state")
			}
		}

		// Send any pending end of stream
		pendingEndOfStreamState := storage.NewValueState(tx.WithPrefix(pendingEndOfStreamPrefix))

		var phantom octosql.Value
		err = pendingEndOfStreamState.Get(&phantom)
		if err == storage.ErrNotFound {
		} else if err != nil {
			return 0, errors.Wrap(err, "couldn't get pending end of stream state")
		} else {

			outputQueue := NewOutputQueue(tx.WithPrefix(outputQueuePrefix))
			if err := outputQueue.Push(ctx, &QueueElement{
				Type: &QueueElement_EndOfStream{
					EndOfStream: true,
				},
			}); err != nil {
				return 0, errors.Wrap(err, "couldn't push end of stream to output queue")
			}
		}

		return 0, nil
	}

	for _, key := range keys {
		records, err := p.processFunction.Trigger(ctx, tx, key)
		if err != nil {
			return 0, errors.Wrap(err, "couldn't trigger process function")
		}

		for i := range records {
			err := outputQueue.Push(ctx, &QueueElement{
				Type: &QueueElement_Record{
					Record: records[i],
				},
			})
			if err != nil {
				return 0, errors.Wrap(err, "couldn't push record to output queue")
			}
		}
	}

	return len(keys), nil
}

func (p *ProcessByKey) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	outputWatermarkState := storage.NewValueState(tx.WithPrefix(outputWatermarkPrefix))
	var octoWatermark octosql.Value
	err := outputWatermarkState.Get(&octoWatermark)
	if err == storage.ErrNotFound {
		return time.Time{}, nil
	} else if err != nil {
		return time.Time{}, errors.Wrap(err, "couldn't get output watermark")
	}

	return octoWatermark.AsTime(), nil
}

func (p *ProcessByKey) MarkEndOfStream(ctx context.Context, tx storage.StateTransaction) error {
	phantom := octosql.MakePhantom()
	pendingEndOfStreamState := storage.NewValueState(tx.WithPrefix(pendingEndOfStreamPrefix))
	if err := pendingEndOfStreamState.Set(&phantom); err != nil {
		return errors.Wrap(err, "couldn't set pending end of stream state")
	}
	return nil
}

func (p *ProcessByKey) MarkError(ctx context.Context, tx storage.StateTransaction, err error) error {
	outputQueue := NewOutputQueue(tx.WithPrefix(outputQueuePrefix))
	err = outputQueue.Push(
		ctx,
		&QueueElement{
			Type: &QueueElement_Error{
				Error: err.Error(),
			},
		},
	)
	if err != nil {
		return errors.Wrap(err, "couldn't push error to output queue")
	}
	return nil
}

func (p *ProcessByKey) ReadyForMore(ctx context.Context, tx storage.StateTransaction) error {
	return nil
}

func (p *ProcessByKey) Close(ctx context.Context, storage storage.Storage) error {
	p.garbageCollectorCtxCancel()
	err := <-p.garbageCollectorCloseErrChan
	if err == context.Canceled || err == context.DeadlineExceeded {
	} else if err != nil {
		return errors.Wrap(err, "couldn't stop garbage collector")
	}

	return nil
}
