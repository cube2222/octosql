package streaming

import (
	"context"
	"fmt"
	"log"
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
	KeyFired(ctx context.Context, tx storage.StateTransaction, key octosql.Value) error
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

func (ct *CountingTrigger) RecordReceived(ctx context.Context, tx storage.StateTransaction, key octosql.Value, eventTime time.Time) error {
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
	if count == ct.fireEvery {
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

func (ct *CountingTrigger) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	return nil
}

func (ct *CountingTrigger) PollKeyToFire(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
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

func (ct *CountingTrigger) KeyFired(ctx context.Context, tx storage.StateTransaction, key octosql.Value) error {
	keyCounts := storage.NewMap(tx.WithPrefix(keyCountsPrefix))

	var countValue octosql.Value
	err := keyCounts.Get(&key, &countValue)
	if err == nil {
		err := keyCounts.Delete(&key)
		if err != nil {
			return errors.Wrap(err, "couldn't delete current count for key")
		}
		return nil
	} else if err != storage.ErrKeyNotFound {
		return errors.Wrap(err, "couldn't get current count for key")
	}

	toSend := storage.NewValueState(tx.WithPrefix(toSendPrefix))

	var out octosql.Value
	err = toSend.Get(&out)
	if err == nil && octosql.AreEqual(key, out) {
		err := toSend.Clear()
		if err != nil {
			return errors.Wrap(err, "couldn't delete key to send")
		}
		return nil
	} else if err != storage.ErrKeyNotFound {
		return errors.Wrap(err, "couldn't get value to send")
	}

	return nil
}

var timeSortedKeys = []byte("$time_sorted_keys$")

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
		if err == storage.ErrKeyNotFound {
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
	if err != nil && err != storage.ErrKeyNotFound {
		return errors.Wrap(err, "couldn't delete send time for key")
	}

	return nil
}

var watermarkPrefix = []byte("$watermark$")

type WatermarkTrigger struct {
}

func NewWatermarkTrigger() *WatermarkTrigger {
	return &WatermarkTrigger{}
}

func (wt *WatermarkTrigger) RecordReceived(ctx context.Context, tx storage.StateTransaction, key octosql.Value, eventTime time.Time) error {
	timeKeys := NewTimeSortedKeys(tx.WithPrefix(timeSortedKeys))
	watermarkStorage := storage.NewValueState(tx.WithPrefix(watermarkPrefix))

	var octoWatermark octosql.Value
	var watermark time.Time
	err := watermarkStorage.Get(&octoWatermark)
	if err == nil {
		watermark = octoWatermark.AsTime()
	} else if err != storage.ErrKeyNotFound {
		return errors.Wrap(err, "couldn't get current watermark")
	}

	// TODO: Maybe add ResetKey to triggers, for when key is triggered in one trigger.
	if watermark.After(eventTime) {
		// TODO: Handling late data
		log.Printf("late data...? %v %v", key, eventTime)
		return nil
	}

	err = timeKeys.Update(key, eventTime)
	if err != nil {
		return errors.Wrap(err, "couldn't update trigger time for key")
	}

	return nil
}

func (wt *WatermarkTrigger) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	watermarkStorage := storage.NewValueState(tx.WithPrefix(watermarkPrefix))

	octoWatermark := octosql.MakeTime(watermark)
	err := watermarkStorage.Set(&octoWatermark)
	if err != nil {
		return errors.Wrap(err, "couldn't set new watermark value")
	}

	return nil
}

func (wt *WatermarkTrigger) PollKeyToFire(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	timeKeys := NewTimeSortedKeys(tx.WithPrefix(timeSortedKeys))
	watermarkStorage := storage.NewValueState(tx.WithPrefix(watermarkPrefix))

	key, sendTime, err := timeKeys.GetFirst()
	if err != nil {
		if err == storage.ErrKeyNotFound {
			return octosql.ZeroValue(), ErrNoKeyToFire
		}
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get first key by time")
	}

	var octoWatermark octosql.Value
	var watermark time.Time
	err = watermarkStorage.Get(&octoWatermark)
	if err == nil {
		watermark = octoWatermark.AsTime()
	} else if err != storage.ErrKeyNotFound {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get current watermark")
	}

	if watermark.Before(sendTime) {
		return octosql.ZeroValue(), ErrNoKeyToFire
	}

	err = timeKeys.Delete(key, sendTime)
	if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't delete key")
	}

	return key, nil
}

func (wt *WatermarkTrigger) KeyFired(ctx context.Context, tx storage.StateTransaction, key octosql.Value) error {
	// We don't want to clear the watermark trigger.
	// Keys should always be triggered when the watermark surpasses their event time.
	return nil
}

var byTimeAndKeyPrefix = []byte("$by_time_and_key$")
var byKeyToTimePrefix = []byte("$by_key_to_time$")

type TimeSortedKeys struct {
	tx storage.StateTransaction
}

func NewTimeSortedKeys(tx storage.StateTransaction) *TimeSortedKeys {
	return &TimeSortedKeys{
		tx: tx,
	}
}

func (tsk *TimeSortedKeys) Update(key octosql.Value, t time.Time) error {
	byTimeAndKey := storage.NewMap(tsk.tx.WithPrefix(byTimeAndKeyPrefix))
	byKeyToTime := storage.NewMap(tsk.tx.WithPrefix(byKeyToTimePrefix))

	var oldTime octosql.Value
	err := byKeyToTime.Get(&key, &oldTime)
	if err == nil {
		oldTimeKey := octosql.MakeTuple([]octosql.Value{oldTime, key})
		err := byTimeAndKey.Delete(&oldTimeKey)
		if err != nil {
			return errors.Wrap(err, "couldn't delete old time for key")
		}
	} else if err == storage.ErrKeyNotFound {
	} else {
		return errors.Wrap(err, "couldn't get old time for key")
	}

	octoTime := octosql.MakeTime(t)

	newTimeKey := octosql.MakeTuple([]octosql.Value{octoTime, key})
	null := octosql.MakeNull()
	err = byTimeAndKey.Set(&newTimeKey, &null)
	if err != nil {
		return errors.Wrap(err, "couldn't set new time key")
	}

	err = byKeyToTime.Set(&key, &octoTime)
	if err != nil {
		return errors.Wrap(err, "couldn't set new time for key")
	}

	return nil
}

func (tsk *TimeSortedKeys) GetFirst() (octosql.Value, time.Time, error) {
	byTimeAndKey := storage.NewMap(tsk.tx.WithPrefix(byTimeAndKeyPrefix))

	iter := byTimeAndKey.GetIterator()
	var key octosql.Value
	var value octosql.Value
	err := iter.Next(&key, &value)
	if err := iter.Close(); err != nil {
		return octosql.ZeroValue(), time.Time{}, errors.Wrap(err, "couldn't close iterator")
	}
	if err != nil {
		if err == storage.ErrEndOfIterator {
			return octosql.ZeroValue(), time.Time{}, storage.ErrKeyNotFound
		}
		return octosql.ZeroValue(), time.Time{}, errors.Wrap(err, "couldn't get first element from iterator")
	}

	if key.GetType() != octosql.TypeTuple {
		return octosql.ZeroValue(), time.Time{}, fmt.Errorf("storage corruption, expected tuple key, got %v", key.GetType())
	}

	tuple := key.AsSlice()

	if len(tuple) != 2 {
		return octosql.ZeroValue(), time.Time{}, fmt.Errorf("storage corruption, expected tuple of length 2, got %v", len(tuple))
	}

	if tuple[0].GetType() != octosql.TypeTime {
		return octosql.ZeroValue(), time.Time{}, fmt.Errorf("storage corruption, expected time in first element of tuple, got %v", tuple[0].GetType())
	}

	t := tuple[0].AsTime()

	return tuple[1], t, nil
}

func (tsk *TimeSortedKeys) DeleteByKey(key octosql.Value) error {
	byKeyToTime := storage.NewMap(tsk.tx.WithPrefix(byKeyToTimePrefix))

	var t octosql.Value
	err := byKeyToTime.Get(&key, &t)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			return storage.ErrKeyNotFound
		}
		return errors.Wrap(err, "couldn't get send time for key")
	}

	return tsk.Delete(key, t.AsTime())
}

func (tsk *TimeSortedKeys) Delete(key octosql.Value, t time.Time) error {
	byTimeAndKey := storage.NewMap(tsk.tx.WithPrefix(byTimeAndKeyPrefix))
	byKeyToTime := storage.NewMap(tsk.tx.WithPrefix(byKeyToTimePrefix))

	newTimeKey := octosql.MakeTuple([]octosql.Value{octosql.MakeTime(t), key})
	err := byTimeAndKey.Delete(&newTimeKey)
	if err != nil {
		return errors.Wrap(err, "couldn't delete old time to send key")
	}
	err = byKeyToTime.Delete(&key)
	if err != nil {
		return errors.Wrap(err, "couldn't delete time to send for key")
	}

	return nil
}
