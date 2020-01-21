package execution

import (
	"context"
	"log"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
	"github.com/cube2222/octosql/streaming/trigger"
)

type ProcessFunction interface {
	AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, key octosql.Value, record *Record) error
	Trigger(ctx context.Context, tx storage.StateTransaction, key octosql.Value) ([]*Record, error) // New Records and Retractions
}

type ProcessByKey struct {
	stateStorage storage.Storage

	trigger         trigger.Trigger
	eventTimeField  octosql.VariableName // Empty if not grouping by event time.
	keyExpression   []Expression
	processFunction ProcessFunction
	variables       octosql.Variables
}

func (p *ProcessByKey) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, record *Record) error {
	variables, err := p.variables.MergeWith(record.AsVariables())
	if err != nil {
		return errors.Wrap(err, "couldn't merge stream variables with record")
	}

	key := make([]octosql.Value, len(p.keyExpression))
	for i := range p.keyExpression {
		key[i], err = p.keyExpression[i].ExpressionValue(ctx, variables)
		if err != nil {
			return errors.Wrapf(err, "couldn't evaluate process key expression with index %v", i)
		}
	}

	keyTuple := octosql.MakeTuple(key)
	err = p.processFunction.AddRecord(ctx, tx, inputIndex, keyTuple, record)
	if err != nil {
		return errors.Wrap(err, "couldn't add record to process function")
	}

	// TODO: This probably has to be decided at runtime
	eventTime := maxWatermark
	if len(p.eventTimeField) > 0 {
		eventTime = record.EventTime().AsTime()
	}

	err = p.trigger.RecordReceived(ctx, tx, keyTuple, eventTime)
	if err != nil {
		return errors.Wrap(err, "couldn't mark record received in trigger")
	}

	err = p.triggerKeys(ctx, tx)
	if err != nil {
		return errors.Wrap(err, "couldn't trigger keys")
	}

	return nil
}

type outputEntry struct {
	record      *Record
	watermark   *time.Time
	endOfStream bool
}

func (p *ProcessByKey) triggerKeys(ctx context.Context, tx storage.StateTransaction) error {
	outputQueue := NewOutputQueue(p.stateStorage.WithPrefix(outputQueuePrefix), tx.WithPrefix(outputQueuePrefix))

	for key, err := p.trigger.PollKeyToFire(ctx, tx); err != trigger.ErrNoKeyToFire; key, err = p.trigger.PollKeyToFire(ctx, tx) {
		if err != nil {
			return errors.Wrap(err, "couldn't poll trigger for key to fire")
		}

		records, err := p.processFunction.Trigger(ctx, tx, key)
		if err != nil {
			return errors.Wrap(err, "couldn't trigger process function")
		}

		for i := range records {
			log.Printf("triggerKeys: record: %s", records[i].Show())
			err := outputQueue.Push(ctx, &QueueElement{
				Type: &QueueElement_Record{
					Record: records[i],
				},
			})
			if err != nil {
				return errors.Wrap(err, "couldn't push record to output queue")
			}
		}
	}

	return nil
}

var outputWatermarkPrefix = []byte("$output_watermark$")
var endOfStreamPrefix = []byte("$end_of_stream$")
var outputQueuePrefix = []byte("$output_queue$")

func (p *ProcessByKey) Next(ctx context.Context, tx storage.StateTransaction) (*Record, error) {
	outputQueue := NewOutputQueue(p.stateStorage.WithPrefix(outputQueuePrefix), tx.WithPrefix(outputQueuePrefix))

	endOfStreamState := storage.NewValueState(tx.WithPrefix(endOfStreamPrefix))
	var eos octosql.Value
	err := endOfStreamState.Get(&eos)
	if err == storage.ErrNotFound {
	} else if err != nil {
		return nil, errors.Wrap(err, "couldn't get end of stream value")
	} else {
		return nil, ErrEndOfStream
	}

	var element *QueueElement
	for element, err = outputQueue.Pop(ctx); err == nil; element, err = outputQueue.Pop(ctx) {
		switch payload := element.Type.(type) {
		case *QueueElement_EndOfStream:
			octoEndOfStream := octosql.MakeBool(true)
			err := endOfStreamState.Set(&octoEndOfStream)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't update end of stream state")
			}
			return nil, ErrEndOfStream
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
		default:
			panic("invalid queue element type")
		}
	}

	return nil, errors.Wrap(err, "couldn't pop element from output queue")
}

func (p *ProcessByKey) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	outputQueue := NewOutputQueue(p.stateStorage.WithPrefix(outputQueuePrefix), tx.WithPrefix(outputQueuePrefix))

	err := p.trigger.UpdateWatermark(ctx, tx, watermark)
	if err != nil {
		return errors.Wrap(err, "couldn't update watermark in trigger")
	}

	err = p.triggerKeys(ctx, tx)
	if err != nil {
		return errors.Wrap(err, "couldn't trigger keys")
	}

	log.Printf("triggerKeys: watermark: %v", watermark)
	timestamp, err := ptypes.TimestampProto(watermark)
	if err != nil {
		return errors.Wrap(err, "couldn't convert time to proto timestamp")
	}
	err = outputQueue.Push(ctx, &QueueElement{Type: &QueueElement_Watermark{Watermark: timestamp}})
	if err != nil {
		return errors.Wrap(err, "couldn't push item to output queue")
	}

	return nil
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
	outputQueue := NewOutputQueue(p.stateStorage.WithPrefix(outputQueuePrefix), tx.WithPrefix(outputQueuePrefix))
	err := outputQueue.Push(ctx, &QueueElement{Type: &QueueElement_EndOfStream{EndOfStream: true}})
	if err != nil {
		return errors.Wrap(err, "couldn't push item to output queue")
	}
	return nil
}

func (p *ProcessByKey) Close() error {
	panic("implement me")
}

type OutputQueue struct {
	stateStorage storage.Storage
	tx           storage.StateTransaction
}

func NewOutputQueue(stateStorage storage.Storage, tx storage.StateTransaction) *OutputQueue {
	return &OutputQueue{
		stateStorage: stateStorage,
		tx:           tx,
	}
}

var queueElementsPrefix = []byte("$queue_elements$")

func (q *OutputQueue) Push(ctx context.Context, element *QueueElement) error {
	queueElements := storage.NewDeque(q.tx.WithPrefix(queueElementsPrefix))

	err := queueElements.PushBack(element)
	if err != nil {
		return errors.Wrap(err, "couldn't append element to queue")
	}
	return nil
}

func (q *OutputQueue) Pop(ctx context.Context) (*QueueElement, error) {
	queueElements := storage.NewDeque(q.tx.WithPrefix(queueElementsPrefix))

	var element QueueElement
	err := queueElements.PopFront(&element)
	if err == storage.ErrNotFound {
		prefixedStorage := q.stateStorage.WithPrefix(queueElementsPrefix)
		subscription := prefixedStorage.Subscribe(ctx)

		curTx := prefixedStorage.BeginTransaction()
		defer curTx.Abort()
		curQueueElements := storage.NewDeque(curTx.WithPrefix(queueElementsPrefix))

		var element QueueElement
		err := curQueueElements.PeekFront(&element)
		if err == storage.ErrNotFound {
			return nil, NewErrWaitForChanges(subscription)
		} else {
			if subErr := subscription.Close(); subErr != nil {
				return nil, errors.Wrap(subErr, "couldn't close subscription")
			}
			if err == nil {
				return nil, ErrNewTransactionRequired
			} else {
				return nil, errors.Wrap(err, "couldn't check if there are elements in the queue out of transaction")
			}
		}
	} else if err != nil {
		return nil, errors.Wrap(err, "couldn't pop element from queue")
	}
	log.Println("ok")

	return &element, nil
}
