package streaming

import (
	"context"
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
	output          chan *execution.Record // TODO: Temporary
	trigger         trigger.Trigger
	byEventTime     bool
	keyExpression   []execution.Expression
	processFunction ProcessFunction
	variables       octosql.Variables
}

func (p *ProcessByKey) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, record *execution.Record) error {
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
	if p.byEventTime {
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
	for key, err := p.trigger.PollKeyToFire(ctx, tx); err != trigger.ErrNoKeyToFire; key, err = p.trigger.PollKeyToFire(ctx, tx) {
		if err != nil {
			return errors.Wrap(err, "couldn't poll trigger for key to fire")
		}

		records, err := p.processFunction.Trigger(ctx, tx, key)
		if err != nil {
			return errors.Wrap(err, "couldn't trigger process function")
		}

		for i := range records {
			p.output <- records[i]
		}
	}

	return nil
}

func (p *ProcessByKey) Next(ctx context.Context, tx storage.StateTransaction) (*execution.Record, error) {
	for record := range p.output {
		return record, nil
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

	return nil
}

func (p *ProcessByKey) Close() error {
	panic("implement me")
}
