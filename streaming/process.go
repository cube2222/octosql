package streaming

import (
	"context"
	"log"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/streaming/storage"
	"github.com/cube2222/octosql/streaming/trigger"
)

type ProcessFunction interface {
	AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, key octosql.Value, record *execution.Record) error
	Trigger(ctx context.Context, tx storage.StateTransaction, key octosql.Value) ([]*execution.Record, error) // New Records and Retractions
}

type ProcessByKey struct {
	output          chan outputEntry // TODO: Temporary
	trigger         trigger.Trigger
	eventTimeField  octosql.VariableName // Empty if not grouping by event time.
	keyExpression   []execution.Expression
	processFunction ProcessFunction
	variables       octosql.Variables
}

func (p *ProcessByKey) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, record *execution.Record) error {
	log.Println("ProcessByKey.AddRecord")
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
	record      *execution.Record
	watermark   *time.Time
	endOfStream bool
}

func (p *ProcessByKey) triggerKeys(ctx context.Context, tx storage.StateTransaction) error {
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
			p.output <- outputEntry{record: records[i]}
		}
	}

	return nil
}

var outputWatermarkPrefix = []byte("$output_watermark$")
var endOfStreamPrefix = []byte("$end_of_stream$")

func (p *ProcessByKey) Next(ctx context.Context, tx storage.StateTransaction) (*execution.Record, error) {
	endOfStreamState := storage.NewValueState(tx.WithPrefix(endOfStreamPrefix))
	var eos octosql.Value
	err := endOfStreamState.Get(&eos)
	if err == storage.ErrKeyNotFound {
	} else if err != nil {
		return nil, errors.Wrap(err, "couldn't get end of stream value")
	} else {
		return nil, execution.ErrEndOfStream
	}

	for record := range p.output {
		if record.endOfStream {
			octoEndOfStream := octosql.MakeBool(true)
			err := endOfStreamState.Set(&octoEndOfStream)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't update end of stream state")
			}
			return nil, execution.ErrEndOfStream
		} else if record.record != nil {
			return record.record, nil
		} else if record.watermark != nil {
			outputWatermarkState := storage.NewValueState(tx.WithPrefix(outputWatermarkPrefix))
			octoWatermark := octosql.MakeTime(*record.watermark)
			err := outputWatermarkState.Set(&octoWatermark)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't update output watermark")
			}
		} else {
			panic("unreachable")
		}
	}
	return nil, execution.ErrEndOfStream
}

func (p *ProcessByKey) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	err := p.trigger.UpdateWatermark(ctx, tx, watermark)
	if err != nil {
		return errors.Wrap(err, "couldn't update watermark in trigger")
	}

	err = p.triggerKeys(ctx, tx)
	if err != nil {
		return errors.Wrap(err, "couldn't trigger keys")
	}

	log.Printf("triggerKeys: watermark: %v", watermark)
	p.output <- outputEntry{watermark: &watermark}

	return nil
}

func (p *ProcessByKey) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	outputWatermarkState := storage.NewValueState(tx.WithPrefix(outputWatermarkPrefix))
	var octoWatermark octosql.Value
	err := outputWatermarkState.Get(&octoWatermark)
	if err == storage.ErrKeyNotFound {
		return time.Time{}, nil
	} else if err != nil {
		return time.Time{}, errors.Wrap(err, "couldn't get output watermark")
	}

	return octoWatermark.AsTime(), nil
}

func (p *ProcessByKey) MarkEndOfStream(ctx context.Context, tx storage.StateTransaction) error {
	p.output <- outputEntry{endOfStream: true}
	return nil
}

func (p *ProcessByKey) Close() error {
	panic("implement me")
}
