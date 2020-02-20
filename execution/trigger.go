package execution

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution/trigger"
)

type TriggerPrototype interface {
	Get(ctx context.Context, variables octosql.Variables) (Trigger, error)
}

type CountingTrigger struct {
	count Expression
}

func NewCountingTrigger(count Expression) *CountingTrigger {
	return &CountingTrigger{count: count}
}

func (c *CountingTrigger) Get(ctx context.Context, variables octosql.Variables) (Trigger, error) {
	count, err := c.count.ExpressionValue(ctx, variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get count expression value")
	}
	if t := count.GetType(); t != octosql.TypeInt {
		return nil, errors.Errorf("counting trigger argument must be int, got %v", t)
	}
	return trigger.NewCountingTrigger(count.AsInt()), nil
}

type DelayTrigger struct {
	delay Expression
}

func NewDelayTrigger(delay Expression) *DelayTrigger {
	return &DelayTrigger{delay: delay}
}

func (c *DelayTrigger) Get(ctx context.Context, variables octosql.Variables) (Trigger, error) {
	delay, err := c.delay.ExpressionValue(ctx, variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get delay expression value")
	}
	if t := delay.GetType(); t != octosql.TypeDuration {
		return nil, errors.Errorf("delay trigger argument must be duration, got %v", t)
	}
	return trigger.NewDelayTrigger(delay.AsDuration(), func() time.Time {
		return time.Now()
	}), nil
}

type WatermarkTrigger struct {
}

func NewWatermarkTrigger() *WatermarkTrigger {
	return &WatermarkTrigger{}
}

func (c *WatermarkTrigger) Get(ctx context.Context, variables octosql.Variables) (Trigger, error) {
	return trigger.NewWatermarkTrigger(), nil
}

type MultiTrigger struct {
	triggers []TriggerPrototype
}

func NewMultiTrigger(triggers ...TriggerPrototype) *MultiTrigger {
	return &MultiTrigger{triggers: triggers}
}

func (m *MultiTrigger) Get(ctx context.Context, variables octosql.Variables) (Trigger, error) {
	triggers := make([]trigger.Trigger, len(m.triggers))
	for i := range m.triggers {
		t, err := m.triggers[i].Get(ctx, variables)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't get trigger from trigger prototype with index %d", i)
		}
		triggers[i] = t
	}
	return trigger.NewMultiTrigger(triggers...), nil
}
