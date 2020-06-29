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

var eventTimesSeenPrefix = []byte("event_times_seen")                         // used to keep track of every event time seen and to collect garbage
var eventTimesPrefixedSetsPrefix = []byte("event_times_prefixed_sets_prefix") // used to collect keys seen in prefixed sets

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
	eventTimePrefixedTx := tx
	if len(p.eventTimeField) > 0 {
		eventTime = record.EventTime().AsTime()

		// Adding record event time to keyTuple so that during Trigger prefixing process function is possible
		keyTuple = octosql.MakeTuple(append([]octosql.Value{octosql.MakeTime(eventTime)}, key...))

		// Adding keyTuple to event time storage
		// For every record seen we store keys that appeared in that event time. It gives us an opportunity to
		// clear/mark as fired old records, because both processFunction and trigger receives key or keyTuple to perform actions.
		eventTimeSet := storage.NewMap(tx.WithPrefix(eventTimesSeenPrefix))

		// First, insert event time to set if it's not present
		octoEventTime := octosql.MakeTime(eventTime)
		var phantom octosql.Value
		err := eventTimeSet.Get(&octoEventTime, &phantom)
		if err == storage.ErrNotFound {
			phantom := octosql.MakePhantom()
			err := eventTimeSet.Set(&octoEventTime, &phantom)
			if err != nil {
				return errors.Wrap(err, "couldn't insert event time to storage")
			}
		} else if err != nil {
			return errors.Wrap(err, "couldn't get event time from storage")
		}

		eventTimeBytes := append(append([]byte("$"), octoEventTime.MonotonicMarshal()...), '$')

		// Now, insert keyTuple to multiset prefixed by event time
		keyTuplesSet := storage.NewMultiSet(tx.WithPrefix(eventTimesPrefixedSetsPrefix).WithPrefix(eventTimeBytes))

		err = keyTuplesSet.Insert(keyTuple)
		if err != nil {
			return errors.Wrap(err, "couldn't insert event time to storage")
		}

		eventTimePrefixedTx = tx.WithPrefix(eventTimeBytes)
	}

	// For process function, garbage collector will just drop event time prefixes, that's why tx is prefixed here
	err = p.processFunction.AddRecord(ctx, eventTimePrefixedTx, inputIndex, keyTuple, record)
	if err != nil {
		return errors.Wrap(err, "couldn't add record to process function")
	}

	// For trigger, garbage collector will call KeysFired so that trigger itself can remove records from storage, that's why tx is NOT prefixed here
	err = p.trigger.RecordReceived(ctx, tx, keyTuple, eventTime)
	if err != nil {
		return errors.Wrap(err, "couldn't mark record received in trigger")
	}

	return nil
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
		eventTimePrefixedTx := tx
		if len(p.eventTimeField) > 0 {
			// Prefixing transaction for process function with record event time
			octoEventTime := key.AsSlice()[0]
			eventTimeBytes := append(append([]byte("$"), octoEventTime.MonotonicMarshal()...), '$')
			eventTimePrefixedTx = tx.WithPrefix(eventTimeBytes)
		}

		records, err := p.processFunction.Trigger(ctx, eventTimePrefixedTx, key)
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

	if err := storage.DropAll(outputWatermarkPrefix); err != nil {
		return errors.Wrap(err, "couldn't clear storage with output watermark prefix")
	}

	return nil
}

type GarbageCollectorInfo struct {
	garbageCollectorBoundary time.Duration // number of seconds subtracted from watermark to create boundary for garbage collection
	garbageCollectorCycle    time.Duration // number of seconds for garbage collector to sleep between records check
}

func NewGarbageCollectorInfo(garbageCollectorBoundary, garbageCollectorCycle time.Duration) *GarbageCollectorInfo {
	return &GarbageCollectorInfo{
		garbageCollectorBoundary: garbageCollectorBoundary,
		garbageCollectorCycle:    garbageCollectorCycle,
	}
}

func (p *ProcessByKey) RunGarbageCollector(ctx context.Context, prefixedStorage storage.Storage, gbBoundary, gbCycle time.Duration) error {
	for range time.Tick(gbCycle * time.Second) {
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
			// Collect every event time earlier than watermark - boundary (by default: watermark - 10min)
			boundary := watermark.Add(-1 * gbBoundary * time.Second)

			eventTimeSet := storage.NewMap(tx.WithPrefix(eventTimesSeenPrefix))
			octoEventTimeSlice := make([]octosql.Value, 0)
			eventTimesBytesSlice := make([][]byte, 0)

			var octoEventTime octosql.Value

			// Iterating through event times seen
			setIt := eventTimeSet.GetIterator()
			var err error
			var phantom octosql.Value
			for err = setIt.Next(&octoEventTime, &phantom); err == nil; err = setIt.Next(&octoEventTime, &phantom) {
				eventTime := octoEventTime.AsTime()

				// Just store event time, clearing process can't happen here because of 2nd iterator creation
				if eventTime.Before(boundary) {
					eventTimeBytes := append(append([]byte("$"), octoEventTime.MonotonicMarshal()...), '$')

					octoEventTimeSlice = append(octoEventTimeSlice, octoEventTime)
					eventTimesBytesSlice = append(eventTimesBytesSlice, eventTimeBytes)
				} else {
					break
				}
			}
			if err == storage.ErrEndOfIterator {
			} else if err != nil {
				return errors.Wrap(err, "couldn't get value from event time iterator")
			}
			if err := setIt.Close(); err != nil {
				return errors.Wrap(err, "couldn't close event time set iterator")
			}

			// Drop every "old enough" event time from event time set and clear prefixed sets
			for i := range octoEventTimeSlice {
				eventTime := octoEventTimeSlice[i].AsTime()
				eventTimeBytes := eventTimesBytesSlice[i]

				// Iterating through all keyTuples with that event time
				keyTuplesSet := storage.NewMultiSet(tx.WithPrefix(eventTimesPrefixedSetsPrefix).WithPrefix(eventTimeBytes))

				allKeysTuple, err := keyTuplesSet.ReadAll()
				if err != nil {
					return errors.Wrapf(err, "couldn't get all keys with event time %v", eventTime)
				}

				// Mark every "old enough" key as fired -> trigger will delete this key
				octoAllKeysTuple := octosql.MakeTuple(allKeysTuple)
				if err = p.trigger.KeysFired(ctx, tx, octoAllKeysTuple.AsSlice()); err != nil {
					return errors.Wrap(err, "couldn't mark old enough keys as fired in trigger")
				}

				// Drop event time prefix from process function storage
				if err := prefixedStorage.DropAll(eventTimeBytes); err != nil {
					return errors.Wrapf(err, "couldn't clear storage prefixed wit event time %v", eventTime)
				}

				// Clearing both storages after processing this event time
				if err := eventTimeSet.Delete(&octoEventTimeSlice[i]); err != nil {
					return errors.Wrapf(err, "couldn't delete event time %v from event time map", octoEventTime.AsTime())
				}

				if err := keyTuplesSet.Clear(); err != nil {
					return errors.Wrapf(err, "couldn't delete event time %v from event time map", octoEventTime.AsTime())
				}
			}
		}
	}
	panic("unreachable")
}
