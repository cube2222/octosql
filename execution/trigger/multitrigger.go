package trigger

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/streaming/storage"
)

type MultiTrigger struct {
	prefixes [][]byte
	triggers []execution.Trigger
}

func NewMultiTrigger(triggers ...execution.Trigger) *MultiTrigger {
	prefixes := make([][]byte, len(triggers))
	for i := range triggers {
		prefixes[i] = []byte(fmt.Sprintf("$%d$", i))
	}
	return &MultiTrigger{
		prefixes: prefixes,
		triggers: triggers,
	}
}

func (m *MultiTrigger) RecordReceived(ctx context.Context, tx storage.StateTransaction, key octosql.Value, eventTime time.Time) error {
	for i := range m.triggers {
		err := m.triggers[i].RecordReceived(ctx, tx.WithPrefix(m.prefixes[i]), key, eventTime)
		if err != nil {
			return errors.Wrapf(err, "couldn't mark record received in trigger with index %d: %v", i, m.triggers[i])
		}
	}

	return nil
}

func (m *MultiTrigger) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	for i := range m.triggers {
		err := m.triggers[i].UpdateWatermark(ctx, tx.WithPrefix(m.prefixes[i]), watermark)
		if err != nil {
			return errors.Wrapf(err, "couldn't update watermark in trigger with index %d: %v", i, m.triggers[i])
		}
	}

	return nil
}

func (m *MultiTrigger) PollKeyToFire(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	for i := range m.triggers {
		key, err := m.triggers[i].PollKeyToFire(ctx, tx.WithPrefix(m.prefixes[i]))
		if err == nil {
			for j := range m.triggers {
				if j != i {
					err := m.triggers[i].KeyFired(ctx, tx.WithPrefix(m.prefixes[j]), key)
					if err != nil {
						return octosql.ZeroValue(), errors.Wrapf(err, "couldn't mark key fired in trigger with index %d after trigger with index %d fired", j, i)
					}
				}
			}
			return key, nil
		} else if err != execution.ErrNoKeyToFire {
			return octosql.ZeroValue(), errors.Wrapf(err, "couldn't poll key to fire in trigger with index %d: %v", i, m.triggers[i])
		}
	}

	return octosql.ZeroValue(), execution.ErrNoKeyToFire
}

func (m *MultiTrigger) KeyFired(ctx context.Context, tx storage.StateTransaction, key octosql.Value) error {
	for i := range m.triggers {
		err := m.triggers[i].KeyFired(ctx, tx.WithPrefix(m.prefixes[i]), key)
		if err != nil {
			return errors.Wrapf(err, "couldn't mark key fired in trigger with index %d: %v", i, m.triggers[i])
		}
	}

	return nil
}
