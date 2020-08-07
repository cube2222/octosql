package trigger

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/storage"
)

type CountingTrigger struct {
	fireEvery int

	keyCounts *storage.Map
	toSend    *storage.ValueState
}

var toSendPrefix = []byte("$to_send$")
var keyCountsPrefix = []byte("$key_counts$")

func NewCountingTrigger(prefix string, fireEvery int) *CountingTrigger {
	return &CountingTrigger{
		fireEvery: fireEvery,
		keyCounts: storage.NewMapFromPrefix(prefix + string(keyCountsPrefix)),
		toSend:    storage.NewValueStateFromPrefix(prefix + string(toSendPrefix)),
	}
}

func (ct *CountingTrigger) Document() docs.Documentation {
	return docs.Section(
		"Counting Trigger",
		docs.Body(
			docs.Section("Description", docs.Text("Triggers after receiving number of records specified in its argument")),
			docs.Section("Arguments", docs.List(
				docs.Text("`fireEvery`: frequency of firing")),
			),
		),
	)
}

func (ct *CountingTrigger) RecordReceived(ctx context.Context, tx storage.StateTransaction, key octosql.Value, eventTime time.Time) error {
	var countValue octosql.Value
	err := ct.keyCounts.Get(&key, &countValue)
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
		err := ct.keyCounts.Delete(&key)
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
		err := ct.keyCounts.Set(&key, &countValue)
		if err != nil {
			return errors.Wrap(err, "couldn't set new count for key")
		}
	}

	return nil
}

func (ct *CountingTrigger) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	return nil
}

func (ct *CountingTrigger) PollKeysToFire(ctx context.Context, tx storage.StateTransaction, batchSize int) ([]octosql.Value, error) {
	toSend := storage.NewValueState(tx.WithPrefix(toSendPrefix))
	var out octosql.Value
	err := toSend.Get(&out)
	if err != nil {
		if err == storage.ErrNotFound {
			return nil, nil
		}
		return nil, errors.Wrap(err, "couldn't get value to send")
	}

	err = toSend.Clear()
	if err != nil {
		return nil, errors.Wrap(err, "couldn't clear value to send")
	}
	return []octosql.Value{out}, nil
}

func (ct *CountingTrigger) KeysFired(ctx context.Context, tx storage.StateTransaction, keys []octosql.Value) error {
	var out octosql.Value
	var foundToSend bool
	err := ct.toSend.Get(&out)
	if err == nil {
		foundToSend = true
	} else if err != storage.ErrNotFound {
		return errors.Wrap(err, "couldn't get value to send")
	}

	for _, key := range keys {
		var countValue octosql.Value
		err := ct.keyCounts.Get(&key, &countValue)
		if err == nil {
			err := ct.keyCounts.Delete(&key)
			if err != nil {
				return errors.Wrap(err, "couldn't delete current count for key")
			}
			return nil
		} else if err != storage.ErrNotFound {
			return errors.Wrap(err, "couldn't get current count for key")
		}

		if foundToSend && octosql.AreEqual(key, out) {
			err := ct.toSend.Clear()
			if err != nil {
				return errors.Wrap(err, "couldn't delete key to send")
			}
			return nil
		}
	}

	return nil
}
