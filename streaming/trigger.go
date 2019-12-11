package streaming

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
)

type Trigger interface {
	RecordReceived(ctx context.Context, tx StateTransaction, key octosql.Value, eventTime time.Time) error
	UpdateWatermark(ctx context.Context, tx StateTransaction, watermark time.Time) error
	PollKeyToFire(ctx context.Context, tx StateTransaction) (octosql.Value, error)
}

type CountingTrigger struct {
	fireEvery int
}

var toSendPrefix = []byte("$to_send$")
var keyCountsPrefix = []byte("$key_counts$")

func (t *CountingTrigger) RecordReceived(ctx context.Context, tx StateTransaction, key octosql.Value, eventTime time.Time) error {
	keyCounts := NewMap(tx.WithPrefix(keyCountsPrefix))

	var countValue octosql.Value
	err := keyCounts.Get(&key, &countValue)
	if err != nil {
		if err == ErrNotFound {
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

		toSend := NewValueState(tx.WithPrefix(toSendPrefix))
		err = toSend.Set(&key)
		if err != nil {
			return errors.Wrap(err, "couldn't append to outbox list")
		}
	}

	return nil
}

func (t *CountingTrigger) UpdateWatermark(ctx context.Context, tx StateTransaction, watermark time.Time) error {
	return nil
}

var ErrNotFound = errors.New("value not found")
var ErrNoKeyToFire = errors.New("no record to send")

func (t *CountingTrigger) PollKeyToFire(ctx context.Context, tx StateTransaction) (octosql.Value, error) {
	toSend := NewValueState(tx.WithPrefix(toSendPrefix))
	var out octosql.Value
	err := toSend.Get(&out)
	if err != nil {
		if err == ErrNotFound {
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
}

var timeToSendPrefix = []byte("$time_time_to_send$")
var whatTimeDoesKeyHavePrefix = []byte("$key_time_to_send$")
var currentWatermarkPrefix = []byte("$currentWatermark$")

func (t *DelayTrigger) RecordReceived(ctx context.Context, tx StateTransaction, key octosql.Value, eventTime time.Time) error {
	keyTimesToSend := NewMap(tx.WithPrefix(timeToSendPrefix))
	whatTimesDoKeysHave := NewMap(tx.WithPrefix(whatTimeDoesKeyHavePrefix))

	var currentWatermarkValue octosql.Value
	err := NewValueState(tx.WithPrefix(currentWatermarkPrefix)).Get(&currentWatermarkValue)
	if err != nil {
		return errors.Wrap(err, "couldn't get current watermark")
	}
	currentWatermark := currentWatermarkValue.AsTime()

	newTimeToSend := eventTime.Add(t.delay)

	if currentWatermark.After(newTimeToSend) {
		return errors.Wrap(t.addKeyToSend(ctx, tx, key), "couldn't add key for immediate send")
	}

	newTimeToSendValue := octosql.MakeTime(newTimeToSend)

	timeToSendKey := octosql.MakeTuple([]octosql.Value{newTimeToSendValue, key})
	null := octosql.MakeNull()
	err = keyTimesToSend.Set(&timeToSendKey, &null)
	if err != nil {
		return errors.Wrap(err, "couldn't set new time to send")
	}

	err = whatTimesDoKeysHave.Set(&key, &newTimeToSendValue)
	if err != nil {
		return errors.Wrap(err, "couldn't set key time to send info")
	}

	return nil
}

func (t *DelayTrigger) addKeyToSend(ctx context.Context, tx StateTransaction, key octosql.Value) error {
	toSend := NewLinkedList(tx.WithPrefix(toSendPrefix))
	return toSend.Append(&key)
}

func (t *DelayTrigger) UpdateWatermark(ctx context.Context, watermark time.Time) error {
	// TODO: with sorted maps
}

func (t *DelayTrigger) PollKeyToFire(ctx context.Context, tx StateTransaction) (octosql.Value, error) {
	toSend := NewLinkedList(tx.WithPrefix(toSendPrefix))
	if toSend.Length() == 0 {
		return octosql.ZeroValue(), ErrNoKeyToFire
	}

	var out octosql.Value
	err := toSend.Pop(0, &out) // Get and delete.
	if ErrNotFound != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get value to send")
	}

	return out, nil
}

// TODO: Chyba jednak pull engine powinien to wywoływac. Inaczej flow staje się strasznie dziwne.

// Tutaj read musi dotknąc storageu, zeby konflikty transakcji dobrze przechodziły.
// Może lepiej po prostu lock na transaction managerze? (tylko w kontekście watermarkow)
type WatermarkStore struct {
}
