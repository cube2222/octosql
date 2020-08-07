package trigger

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

var timeSortedKeys = "$time_sorted_keys$"
var byTimeAndKeyPrefix = "$by_time_and_key$"
var byKeyToTimePrefix = "$by_key_to_time$"

type TimeSortedKeys struct {
	byTimeAndKey *storage.Map
	byKeyToTime  *storage.Map
}

func NewTimeSortedKeys(prefix string) *TimeSortedKeys {
	return &TimeSortedKeys{
		byTimeAndKey: storage.NewMapFromPrefix(prefix + byTimeAndKeyPrefix),
		byKeyToTime:  storage.NewMapFromPrefix(prefix + byKeyToTimePrefix),
	}
}

func (tsk *TimeSortedKeys) Update(key octosql.Value, t time.Time) error {
	var oldTime octosql.Value
	err := tsk.byKeyToTime.Get(&key, &oldTime)
	if err == nil {
		oldTimeKey := octosql.MakeTuple([]octosql.Value{oldTime, key})
		err := tsk.byTimeAndKey.Delete(&oldTimeKey)
		if err != nil {
			return errors.Wrap(err, "couldn't delete old time for key")
		}
	} else if err == storage.ErrNotFound {
	} else {
		return errors.Wrap(err, "couldn't get old time for key")
	}

	octoTime := octosql.MakeTime(t)

	newTimeKey := octosql.MakeTuple([]octosql.Value{octoTime, key})
	null := octosql.MakeNull()
	err = tsk.byTimeAndKey.Set(&newTimeKey, &null)
	if err != nil {
		return errors.Wrap(err, "couldn't set new time key")
	}

	err = tsk.byKeyToTime.Set(&key, &octoTime)
	if err != nil {
		return errors.Wrap(err, "couldn't set new time for key")
	}

	return nil
}

func (tsk *TimeSortedKeys) GetUntil(until time.Time, batchSize int) ([]octosql.Value, []time.Time, error) {
	var outValues []octosql.Value
	var outTimes []time.Time

	iter := tsk.byTimeAndKey.GetIterator()
	var key octosql.Value
	var value octosql.Value
	var err error
	for err = iter.Next(&key, &value); err == nil; err = iter.Next(&key, &value) {
		if key.GetType() != octosql.TypeTuple {
			return nil, nil, fmt.Errorf("storage corruption, expected tuple key, got %v", key.GetType())
		}

		tuple := key.AsSlice()

		if len(tuple) != 2 {
			return nil, nil, fmt.Errorf("storage corruption, expected tuple of length 2, got %v", len(tuple))
		}

		if tuple[0].GetType() != octosql.TypeTime {
			return nil, nil, fmt.Errorf("storage corruption, expected time in first element of tuple, got %v", tuple[0].GetType())
		}

		t := tuple[0].AsTime()
		if t.After(until) {
			break
		}

		if len(outValues) == batchSize {
			break
		}

		outTimes = append(outTimes, tuple[0].AsTime())
		outValues = append(outValues, tuple[1])
	}
	if err == storage.ErrEndOfIterator {
	} else if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get first element from iterator")
	}

	if err := iter.Close(); err != nil {
		return nil, nil, errors.Wrap(err, "couldn't close iterator")
	}

	return outValues, outTimes, nil
}

func (tsk *TimeSortedKeys) GetFirst() (octosql.Value, time.Time, error) {
	iter := tsk.byTimeAndKey.GetIterator()
	var key octosql.Value
	var value octosql.Value
	err := iter.Next(&key, &value)
	if err := iter.Close(); err != nil {
		return octosql.ZeroValue(), time.Time{}, errors.Wrap(err, "couldn't close iterator")
	}
	if err != nil {
		if err == storage.ErrEndOfIterator {
			return octosql.ZeroValue(), time.Time{}, storage.ErrNotFound
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
	var t octosql.Value
	err := tsk.byKeyToTime.Get(&key, &t)
	if err != nil {
		if err == storage.ErrNotFound {
			return storage.ErrNotFound
		}
		return errors.Wrap(err, "couldn't get send time for key")
	}

	return tsk.Delete(key, t.AsTime())
}

func (tsk *TimeSortedKeys) Delete(key octosql.Value, t time.Time) error {
	newTimeKey := octosql.MakeTuple([]octosql.Value{octosql.MakeTime(t), key})
	err := tsk.byTimeAndKey.Delete(&newTimeKey)
	if err != nil {
		return errors.Wrap(err, "couldn't delete old time to send key")
	}
	err = tsk.byKeyToTime.Delete(&key)
	if err != nil {
		return errors.Wrap(err, "couldn't delete time to send for key")
	}

	return nil
}
