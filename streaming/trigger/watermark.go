package trigger

import (
	"context"
	"log"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

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
