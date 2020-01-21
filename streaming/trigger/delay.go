package trigger

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

type DelayTrigger struct {
	delay time.Duration
	clock func() time.Time
}

func NewDelayTrigger(delay time.Duration, clock func() time.Time) *DelayTrigger {
	return &DelayTrigger{
		delay: delay,
		clock: clock,
	}
}

func (dt *DelayTrigger) RecordReceived(ctx context.Context, tx storage.StateTransaction, key octosql.Value, eventTime time.Time) error {
	timeKeys := NewTimeSortedKeys(tx.WithPrefix(timeSortedKeys))

	now := dt.clock()
	sendTime := now.Add(dt.delay)

	err := timeKeys.Update(key, sendTime)
	if err != nil {
		return errors.Wrap(err, "couldn't update trigger time for key")
	}

	return nil
}

func (dt *DelayTrigger) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	return nil
}

func (dt *DelayTrigger) PollKeyToFire(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	timeKeys := NewTimeSortedKeys(tx.WithPrefix(timeSortedKeys))

	key, sendTime, err := timeKeys.GetFirst()
	if err != nil {
		if err == storage.ErrNotFound {
			return octosql.ZeroValue(), ErrNoKeyToFire
		}
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get first key by time")
	}

	if dt.clock().Before(sendTime) {
		return octosql.ZeroValue(), ErrNoKeyToFire
	}

	err = timeKeys.Delete(key, sendTime)
	if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't delete key")
	}

	return key, nil
}

func (dt *DelayTrigger) KeyFired(ctx context.Context, tx storage.StateTransaction, key octosql.Value) error {
	timeKeys := NewTimeSortedKeys(tx.WithPrefix(timeSortedKeys))

	err := timeKeys.DeleteByKey(key)
	if err != nil && err != storage.ErrNotFound {
		return errors.Wrap(err, "couldn't delete send time for key")
	}

	return nil
}
