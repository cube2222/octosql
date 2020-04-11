package execution

import (
	"context"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

type ProcessFunction interface {
	AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, key octosql.Value, record *Record) error
	Trigger(ctx context.Context, tx storage.StateTransaction, key octosql.Value) ([]*Record, error) // New Records and Retractions
}

type Trigger interface {
	RecordReceived(ctx context.Context, tx storage.StateTransaction, key octosql.Value, eventTime time.Time) error
	UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error
	PollKeysToFire(ctx context.Context, tx storage.StateTransaction) ([]octosql.Value, error)
	KeysFired(ctx context.Context, tx storage.StateTransaction, key []octosql.Value) error
}

type ProcessByKey struct {
	trigger         Trigger
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

func (p *ProcessByKey) triggerKeys(ctx context.Context, tx storage.StateTransaction) error {
	outputQueue := NewOutputQueue(tx.WithPrefix(outputQueuePrefix))

	keys, err := p.trigger.PollKeysToFire(ctx, tx)
	if err != nil {
		return errors.Wrap(err, "couldn't poll keys to fire")
	}

	for _, key := range keys {
		records, err := p.processFunction.Trigger(ctx, tx, key)
		if err != nil {
			return errors.Wrap(err, "couldn't trigger process function")
		}

		for i := range records {
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
	outputQueue := NewOutputQueue(tx.WithPrefix(outputQueuePrefix))

	err := p.trigger.UpdateWatermark(ctx, tx, watermark)
	if err != nil {
		return errors.Wrap(err, "couldn't update watermark in trigger")
	}

	err = p.triggerKeys(ctx, tx)
	if err != nil {
		return errors.Wrap(err, "couldn't trigger keys")
	}

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
	outputQueue := NewOutputQueue(tx.WithPrefix(outputQueuePrefix))
	err := outputQueue.Push(ctx, &QueueElement{Type: &QueueElement_EndOfStream{EndOfStream: true}})
	if err != nil {
		return errors.Wrap(err, "couldn't push item to output queue")
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

func (p *ProcessByKey) Close() error {
	return nil // TODO: Close this, remove state
}
