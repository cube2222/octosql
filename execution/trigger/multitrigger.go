package trigger

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/storage"
)

type Trigger interface {
	RecordReceived(ctx context.Context, tx storage.StateTransaction, key octosql.Value, eventTime time.Time) error
	UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error
	PollKeysToFire(ctx context.Context, tx storage.StateTransaction, batchSize int) ([]octosql.Value, error)
	KeysFired(ctx context.Context, tx storage.StateTransaction, key []octosql.Value) error
}

type MultiTrigger struct {
	prefixes [][]byte
	triggers []Trigger
}

func NewMultiTrigger(triggers ...Trigger) *MultiTrigger {
	prefixes := make([][]byte, len(triggers))
	for i := range triggers {
		prefixes[i] = []byte(fmt.Sprintf("$%d$", i))
	}
	return &MultiTrigger{
		prefixes: prefixes,
		triggers: triggers,
	}
}

func (m *MultiTrigger) Document() docs.Documentation {
	return docs.Section(
		"Multitrigger",
		docs.Body(
			docs.Section("Description", docs.Text("Operates on combination of all underlying triggers at once")),
			docs.Section("Arguments", docs.List(
				docs.Text("`triggers`: underlying triggers")),
			),
		),
	)
}

func (m *MultiTrigger) RecordReceived(ctx context.Context, tx storage.StateTransaction, key octosql.Value, eventTime time.Time) error {
	for i := range m.triggers {
		err := m.triggers[i].RecordReceived(ctx, tx.WithPrefix(m.prefixes[i]), key, eventTime)
		if err != nil {
			return errors.Wrapf(err, "couldn't mark record received in Trigger with index %d: %v", i, m.triggers[i])
		}
	}

	return nil
}

func (m *MultiTrigger) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	for i := range m.triggers {
		err := m.triggers[i].UpdateWatermark(ctx, tx.WithPrefix(m.prefixes[i]), watermark)
		if err != nil {
			return errors.Wrapf(err, "couldn't update watermark in Trigger with index %d: %v", i, m.triggers[i])
		}
	}

	return nil
}

func (m *MultiTrigger) PollKeysToFire(ctx context.Context, tx storage.StateTransaction, batchSize int) ([]octosql.Value, error) {
	var outKeys []octosql.Value
	for i := range m.triggers {
		if batchSize == 0 {
			return outKeys, nil
		}
		keys, err := m.triggers[i].PollKeysToFire(ctx, tx.WithPrefix(m.prefixes[i]), batchSize)
		if err == nil && len(keys) > 0 {
			batchSize -= len(keys)
			// In all other triggers, mark these keys as fired.
			// We don't want duplicates in the outKeys slice.
			for j := range m.triggers {
				if j != i {
					err := m.triggers[j].KeysFired(ctx, tx.WithPrefix(m.prefixes[j]), keys)
					if err != nil {
						return nil, errors.Wrapf(err, "couldn't mark keys fired in Trigger with index %d after Trigger with index %d fired", j, i)
					}
				}
			}
			if outKeys == nil { // Just a small premature optimization.
				outKeys = keys
			} else {
				outKeys = append(outKeys, keys...)
			}
		} else if err != nil {
			return nil, errors.Wrapf(err, "couldn't poll keys to fire in Trigger with index %d: %v", i, m.triggers[i])
		}
	}

	return outKeys, nil
}

func (m *MultiTrigger) KeysFired(ctx context.Context, tx storage.StateTransaction, keys []octosql.Value) error {
	for i := range m.triggers {
		err := m.triggers[i].KeysFired(ctx, tx.WithPrefix(m.prefixes[i]), keys)
		if err != nil {
			return errors.Wrapf(err, "couldn't mark keys fired in Trigger with index %d: %v", i, m.triggers[i])
		}
	}

	return nil
}
