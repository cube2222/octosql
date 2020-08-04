package trigger

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/storage"
)

var watermarkPrefix = []byte("$watermark$")

type WatermarkTrigger struct {
}

func NewWatermarkTrigger() *WatermarkTrigger {
	return &WatermarkTrigger{}
}

func (wt *WatermarkTrigger) Document() docs.Documentation {
	return docs.Section(
		"Watermark Trigger",
		docs.Body(
			docs.Section("Description", docs.Text("Triggers every record that has event time smaller than watermark value.")),
			docs.Section("Arguments", docs.List()),
		),
	)
}

func (wt *WatermarkTrigger) RecordReceived(ctx context.Context, tx storage.StateTransaction, key octosql.Value, eventTime time.Time) error {
	timeKeys := NewTimeSortedKeys(tx.WithPrefix(timeSortedKeys))
	watermarkStorage := storage.NewValueState(tx.WithPrefix(watermarkPrefix))

	var octoWatermark octosql.Value
	var watermark time.Time
	err := watermarkStorage.Get(&octoWatermark)
	if err == nil {
		watermark = octoWatermark.AsTime()
	} else if err != storage.ErrNotFound {
		return errors.Wrap(err, "couldn't get current watermark")
	}

	if watermark.After(eventTime) {
		// TODO: Handling late data
		log.Printf("late data...? watermark: %v key: %v event_time: %v", watermark, key.Show(), eventTime)
		return nil
	}

	err = timeKeys.Update(key, eventTime)
	if err != nil {
		return errors.Wrap(err, "couldn't update Trigger time for key")
	}

	return nil
}

var readyToFirePrefix = []byte(fmt.Sprint("$ready_to_fire$"))

func (wt *WatermarkTrigger) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	watermarkStorage := storage.NewValueState(tx.WithPrefix(watermarkPrefix))
	readyToFire := storage.NewValueState(tx.WithPrefix(readyToFirePrefix))

	octoWatermark := octosql.MakeTime(watermark)
	err := watermarkStorage.Set(&octoWatermark)
	if err != nil {
		return errors.Wrap(err, "couldn't set new watermark value")
	}

	ready, err := wt.isSomethingReadyToFire(ctx, tx, watermark)
	if err != nil {
		return errors.Wrap(err, "couldn't check if something is ready to fire")
	}
	if ready {
		octoReady := octosql.MakeBool(true)
		err := readyToFire.Set(&octoReady)
		if err != nil {
			return errors.Wrap(err, "couldn't set ready to fire")
		}
	}

	return nil
}

func (wt *WatermarkTrigger) isSomethingReadyToFire(ctx context.Context, tx storage.StateTransaction, watermark time.Time) (bool, error) {
	timeKeys := NewTimeSortedKeys(tx.WithPrefix(timeSortedKeys))

	_, sendTime, err := timeKeys.GetFirst()
	if err != nil {
		if err == storage.ErrNotFound {
			return false, nil
		}
		return false, errors.Wrap(err, "couldn't get first key by time")
	}

	if watermark.Before(sendTime) {
		return false, nil
	}

	return true, nil
}

func (wt *WatermarkTrigger) PollKeysToFire(ctx context.Context, tx storage.StateTransaction, batchSize int) ([]octosql.Value, error) {
	timeKeys := NewTimeSortedKeys(tx.WithPrefix(timeSortedKeys))
	watermarkStorage := storage.NewValueState(tx.WithPrefix(watermarkPrefix))
	readyToFire := storage.NewValueState(tx.WithPrefix(readyToFirePrefix))

	var octoReady octosql.Value
	err := readyToFire.Get(&octoReady)
	if err == storage.ErrNotFound {
		octoReady = octosql.MakeBool(false)
	} else if err != nil {
		return nil, errors.Wrap(err, "couldn't get readiness to fire value")
	}
	if !octoReady.AsBool() {
		return nil, nil
	}

	var octoWatermark octosql.Value
	var watermark time.Time
	err = watermarkStorage.Get(&octoWatermark)
	if err == nil {
		watermark = octoWatermark.AsTime()
	} else if err != storage.ErrNotFound {
		return nil, errors.Wrap(err, "couldn't get current watermark")
	}

	keys, times, err := timeKeys.GetUntil(watermark, batchSize)
	if err != nil {
		if err == storage.ErrNotFound {
			panic("unreachable")
		}
		return nil, errors.Wrap(err, "couldn't get keys by time until watermark")
	}

	for i := range keys {
		if watermark.Before(times[i]) {
			panic("unreachable")
		}

		err = timeKeys.Delete(keys[i], times[i])
		if err != nil {
			return nil, errors.Wrap(err, "couldn't delete key")
		}
	}

	ready, err := wt.isSomethingReadyToFire(ctx, tx, watermark)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't check if something is ready to fire")
	}
	if !ready {
		octoReady := octosql.MakeBool(false)
		err := readyToFire.Set(&octoReady)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't set ready to fire")
		}
	}

	return keys, nil
}

func (wt *WatermarkTrigger) KeysFired(ctx context.Context, tx storage.StateTransaction, key []octosql.Value) error {
	// We don't want to clear the watermark Trigger.
	// Keys should always be triggered when the watermark surpasses their event time.
	return nil
}
