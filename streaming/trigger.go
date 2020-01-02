package streaming

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

var ErrNoKeyToFire = errors.New("no record to send")

type Trigger interface {
	RecordReceived(ctx context.Context, tx storage.StateTransaction, key octosql.Value, eventTime time.Time) error
	UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error
	PollKeyToFire(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error)
}

type CountingTrigger struct {
	fireEvery int
}

func NewCountingTrigger(fireEvery int) *CountingTrigger {
	return &CountingTrigger{
		fireEvery: fireEvery,
	}
}

var toSendPrefix = []byte("$to_send$")
var keyCountsPrefix = []byte("$key_counts$")

func (t *CountingTrigger) RecordReceived(ctx context.Context, tx storage.StateTransaction, key octosql.Value, eventTime time.Time) error {
	keyCounts := storage.NewMap(tx.WithPrefix(keyCountsPrefix))

	var countValue octosql.Value
	err := keyCounts.Get(&key, &countValue)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			countValue = octosql.MakeInt(0)
		} else {
			return errors.Wrap(err, "couldn't get current count for key")
		}
	}
	count := countValue.AsInt()

	count += 1
	if count == t.fireEvery {
		err := keyCounts.Delete(&key)
		if err != nil {
			return errors.Wrap(err, "couldn't delete current count for key")
		}

		toSend := storage.NewValueState(tx.WithPrefix(toSendPrefix))
		err = toSend.Set(&key)
		if err != nil {
			return errors.Wrap(err, "couldn't append to outbox list")
		}
	} else {
		countValue = octosql.MakeInt(count)
		err := keyCounts.Set(&key, &countValue)
		if err != nil {
			return errors.Wrap(err, "couldn't set new count for key")
		}
	}

	return nil
}

func (t *CountingTrigger) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	return nil
}

func (t *CountingTrigger) PollKeyToFire(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	toSend := storage.NewValueState(tx.WithPrefix(toSendPrefix))
	var out octosql.Value
	err := toSend.Get(&out)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			return octosql.ZeroValue(), ErrNoKeyToFire
		}
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get value to send")
	}

	err = toSend.Clear()
	if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't clear value to send")
	}
	return out, nil
}

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

var timeToSendPrefix = []byte("$time_time_to_send$")
var whatTimeDoesKeyHavePrefix = []byte("$key_time_to_send$")

func (t *DelayTrigger) RecordReceived(ctx context.Context, tx storage.StateTransaction, key octosql.Value, eventTime time.Time) error {
	keyTimesToSend := storage.NewMap(tx.WithPrefix(timeToSendPrefix))
	whatTimesDoKeysHave := storage.NewMap(tx.WithPrefix(whatTimeDoesKeyHavePrefix))

	var oldSendTime octosql.Value
	err := whatTimesDoKeysHave.Get(&key, &oldSendTime)
	if err == nil {
		oldTimeToSendKey := octosql.MakeTuple([]octosql.Value{oldSendTime, key})
		err := keyTimesToSend.Delete(&oldTimeToSendKey)
		if err != nil {
			return errors.Wrap(err, "couldn't delete old time to send for key")
		}
	} else if err == storage.ErrKeyNotFound {
	} else {
		return errors.Wrap(err, "couldn't get old time to send for key")
	}

	now := t.clock()
	sendTime := now.Add(t.delay)
	octoSendTime := octosql.MakeTime(sendTime)

	timeToSendKey := octosql.MakeTuple([]octosql.Value{octoSendTime, key})
	null := octosql.MakeNull()
	err = keyTimesToSend.Set(&timeToSendKey, &null)
	if err != nil {
		return errors.Wrap(err, "couldn't set new time to send")
	}

	err = whatTimesDoKeysHave.Set(&key, &octoSendTime)
	if err != nil {
		return errors.Wrap(err, "couldn't set key time to send")
	}

	return nil
}

func (t *DelayTrigger) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	return nil
}

func (t *DelayTrigger) PollKeyToFire(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	keyTimesToSend := storage.NewMap(tx.WithPrefix(timeToSendPrefix))
	whatTimesDoKeysHave := storage.NewMap(tx.WithPrefix(whatTimeDoesKeyHavePrefix))

	iter := keyTimesToSend.GetIterator()
	var key octosql.Value
	var value octosql.Value
	err := iter.Next(&key, &value)
	if err := iter.Close(); err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't close iterator")
	}
	if err != nil {
		if err == storage.ErrEndOfIterator {
			return octosql.ZeroValue(), ErrNoKeyToFire
		}
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get first element from iterator")
	}

	if key.GetType() != octosql.TypeTuple {
		return octosql.ZeroValue(), fmt.Errorf("storage corruption, expected tuple key, got %v", key.GetType())
	}

	tuple := key.AsSlice()

	if len(tuple) != 2 {
		return octosql.ZeroValue(), fmt.Errorf("storage corruption, expected tuple of length 2, got %v", len(tuple))
	}

	if tuple[0].GetType() != octosql.TypeTime {
		return octosql.ZeroValue(), fmt.Errorf("storage corruption, expected time in first element of tuple, got %v", tuple[0].GetType())
	}

	keyTime := tuple[0].AsTime()

	if keyTime.After(t.clock()) {
		return octosql.ZeroValue(), ErrNoKeyToFire
	}

	err = keyTimesToSend.Delete(&key)
	if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't delete old time to send key")
	}
	err = whatTimesDoKeysHave.Delete(&tuple[1])
	if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't delete time to send for key")
	}

	return tuple[1], nil
}
