package trigger

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

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
		if err == storage.ErrNotFound {
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
		if err == storage.ErrNotFound {
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
	} else if err != storage.ErrNotFound {
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
	} else if err != storage.ErrNotFound {
		return errors.Wrap(err, "couldn't get value to send")
	}

	return nil
}
