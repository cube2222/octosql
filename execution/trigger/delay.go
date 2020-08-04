package trigger

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/storage"
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

func (dt *DelayTrigger) Document() docs.Documentation {
	return docs.Section(
		"Delay Trigger",
		docs.Body(
			docs.Section("Description", docs.Text("Every record it receives got send time equal to `clock() + delay`.\nTriggers every record that has send time smaller than time returned by `clock()`.")),
			docs.Section("Arguments", docs.List(
				docs.Text("`delay`: duration added to every record received"),
				docs.Text("`clock`: function returning time")),
			),
		),
	)
}

func (dt *DelayTrigger) RecordReceived(ctx context.Context, tx storage.StateTransaction, key octosql.Value, eventTime time.Time) error {
	timeKeys := NewTimeSortedKeys(tx.WithPrefix(timeSortedKeys))

	now := dt.clock()
	sendTime := now.Add(dt.delay)

	err := timeKeys.Update(key, sendTime)
	if err != nil {
		return errors.Wrap(err, "couldn't update Trigger time for key")
	}

	return nil
}

func (dt *DelayTrigger) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	return nil
}

func (dt *DelayTrigger) PollKeysToFire(ctx context.Context, tx storage.StateTransaction, batchSize int) ([]octosql.Value, error) {
	timeKeys := NewTimeSortedKeys(tx.WithPrefix(timeSortedKeys))

	now := dt.clock()

	keys, times, err := timeKeys.GetUntil(now, batchSize)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get first key by time")
	}

	for i := range keys {
		err = timeKeys.Delete(keys[i], times[i])
		if err != nil {
			return nil, errors.Wrap(err, "couldn't delete key")
		}
	}

	return keys, nil
}

func (dt *DelayTrigger) KeysFired(ctx context.Context, tx storage.StateTransaction, keys []octosql.Value) error {
	timeKeys := NewTimeSortedKeys(tx.WithPrefix(timeSortedKeys))

	for _, key := range keys {
		err := timeKeys.DeleteByKey(key)
		if err != nil && err != storage.ErrNotFound {
			return errors.Wrap(err, "couldn't delete send time for key")
		}
	}

	return nil
}
