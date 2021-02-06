package physical

import (
	"context"

	"github.com/cube2222/octosql/execution"
)

type Trigger struct {
	TriggerType TriggerType
	// Only one of the below may be non-null.
	CountingTrigger    *CountingTrigger
	EndOfStreamTrigger *EndOfStreamTrigger
	WatermarkTrigger   *WatermarkTrigger
	MultiTrigger       *MultiTrigger
}

type TriggerType int

const (
	TriggerTypeCounting TriggerType = iota
	TriggerTypeEndOfStream
	TriggerTypeWatermark
	TriggerTypeMulti
)

type CountingTrigger struct {
	TriggerAfter uint
}

type EndOfStreamTrigger struct {
}

type WatermarkTrigger struct {
}

type MultiTrigger struct {
	Triggers []Trigger
}

func (t *Trigger) Materialize(ctx context.Context, env Environment) func() execution.Trigger {
	switch t.TriggerType {
	case TriggerTypeCounting:
		return execution.NewCountingTriggerPrototype(t.CountingTrigger.TriggerAfter)
	case TriggerTypeEndOfStream:
		return execution.NewEndOfStreamTriggerPrototype()
	case TriggerTypeWatermark:
		return execution.NewWatermarkTriggerPrototype()
	case TriggerTypeMulti:
		prototypes := make([]func() execution.Trigger, len(t.MultiTrigger.Triggers))
		for i := range t.MultiTrigger.Triggers {
			prototypes[i] = t.MultiTrigger.Triggers[i].Materialize(ctx, env)
		}
		return execution.NewMultiTriggerPrototype(prototypes)
	}

	panic("unexhaustive trigger type match")
}
