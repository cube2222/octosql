package execution

import (
	"fmt"
	"time"

	"github.com/google/btree"
)

type Trigger interface {
	EndOfStreamReached()
	WatermarkReceived(watermark time.Time)
	KeyReceived(key GroupKey)
	Poll() []GroupKey
	// ExpireKeysBeforeTime(time time.Time)
}

type CountingTrigger struct {
	triggerAfter uint

	counts             *btree.BTree
	endOfStreamReached bool
	toTrigger          []GroupKey
}

func NewCountingTriggerPrototype(triggerAfter uint) func() Trigger {
	return func() Trigger {
		return &CountingTrigger{
			triggerAfter:       triggerAfter,
			counts:             btree.New(BTreeDefaultDegree),
			endOfStreamReached: false,
			toTrigger:          []GroupKey{},
		}
	}
}

type countingTriggerItem struct {
	GroupKey
	Count uint
}

func (c *CountingTrigger) EndOfStreamReached() {
	c.endOfStreamReached = true
}

func (c *CountingTrigger) WatermarkReceived(watermark time.Time) {}

func (c *CountingTrigger) KeyReceived(key GroupKey) {
	item := c.counts.Get(key)
	var itemTyped *countingTriggerItem

	if item == nil {
		itemTyped = &countingTriggerItem{GroupKey: key}
		c.counts.ReplaceOrInsert(itemTyped)
	} else {
		var ok bool
		itemTyped, ok = item.(*countingTriggerItem)
		if !ok {
			panic(fmt.Sprintf("invalid received item: %v", item))
		}
	}
	itemTyped.Count++
	if itemTyped.Count == c.triggerAfter {
		c.toTrigger = append(c.toTrigger, itemTyped.GroupKey)
		c.counts.Delete(itemTyped)
	}
}

// The returned slice will be made invalid after following operations on the trigger.
func (c *CountingTrigger) Poll() []GroupKey {
	output := c.toTrigger
	c.toTrigger = c.toTrigger[:0]
	if c.endOfStreamReached {
		c.counts.Ascend(func(item btree.Item) bool {
			itemTyped, ok := item.(*countingTriggerItem)
			if !ok {
				panic(fmt.Sprintf("invalid received item: %v", item))
			}

			output = append(output, itemTyped.GroupKey)

			return true
		})
	}
	return output
}

type watermarkTriggerKey struct {
	Time     time.Time
	GroupKey GroupKey
}

func (key watermarkTriggerKey) Less(than btree.Item) bool {
	thanTyped, ok := than.(watermarkTriggerKey)
	if !ok {
		panic(fmt.Sprintf("invalid key comparison: %T", than))
	}

	if key.Time == thanTyped.Time {
		return key.GroupKey.Less(thanTyped.GroupKey)
	} else {
		return key.Time.Before(thanTyped.Time)
	}
}

type WatermarkTrigger struct {
	timeFieldKeyIndex int

	timeKeys           *btree.BTree
	endOfStreamReached bool
	watermark          time.Time

	outputKeysSlice []GroupKey
}

func NewWatermarkTriggerPrototype(timeFieldKeyIndex int) func() Trigger {
	return func() Trigger {
		return &WatermarkTrigger{
			timeFieldKeyIndex:  timeFieldKeyIndex,
			timeKeys:           btree.New(BTreeDefaultDegree),
			endOfStreamReached: false,
			watermark:          time.Time{},
			outputKeysSlice:    make([]GroupKey, 0),
		}
	}
}

func (c *WatermarkTrigger) EndOfStreamReached() {
	c.endOfStreamReached = true
}

func (c *WatermarkTrigger) WatermarkReceived(watermark time.Time) {
	c.watermark = watermark
}

// TODO: Event time has to be the first element of the key.
func (c *WatermarkTrigger) KeyReceived(key GroupKey) {
	c.timeKeys.ReplaceOrInsert(watermarkTriggerKey{
		Time:     key[c.timeFieldKeyIndex].Time,
		GroupKey: key,
	})
}

// The returned slice will be made invalid after following operations on the trigger.
func (c *WatermarkTrigger) Poll() []GroupKey {
	c.outputKeysSlice = c.outputKeysSlice[:0]
	if !c.endOfStreamReached {
		c.timeKeys.Ascend(func(item btree.Item) bool {
			itemTyped, ok := item.(watermarkTriggerKey)
			if !ok {
				panic(fmt.Sprintf("invalid received item: %v", item))
			}

			if itemTyped.Time.After(c.watermark) {
				return false
			}

			c.outputKeysSlice = append(c.outputKeysSlice, itemTyped.GroupKey)
			c.timeKeys.Delete(item)

			return true
		})
	} else {
		c.timeKeys.Ascend(func(item btree.Item) bool {
			itemTyped, ok := item.(watermarkTriggerKey)
			if !ok {
				panic(fmt.Sprintf("invalid received item: %v", item))
			}

			c.outputKeysSlice = append(c.outputKeysSlice, itemTyped.GroupKey)

			return true
		})
	}
	return c.outputKeysSlice
}

type EndOfStreamTrigger struct {
	keys               *btree.BTree
	endOfStreamReached bool
}

func NewEndOfStreamTriggerPrototype() func() Trigger {
	return func() Trigger {
		return &EndOfStreamTrigger{
			keys:               btree.New(BTreeDefaultDegree),
			endOfStreamReached: false,
		}
	}
}

func (c *EndOfStreamTrigger) EndOfStreamReached() {
	c.endOfStreamReached = true
}

func (c *EndOfStreamTrigger) WatermarkReceived(watermark time.Time) {}

func (c *EndOfStreamTrigger) KeyReceived(key GroupKey) {
	c.keys.ReplaceOrInsert(key)
}

func (c *EndOfStreamTrigger) Poll() []GroupKey {
	if !c.endOfStreamReached {
		return nil
	}
	output := make([]GroupKey, 0, c.keys.Len())
	c.keys.Ascend(func(item btree.Item) bool {
		itemTyped, ok := item.(GroupKey)
		if !ok {
			panic(fmt.Sprintf("invalid received item: %v", item))
		}

		output = append(output, itemTyped)

		return true
	})
	return output
}

type MultiTrigger struct {
}

func NewMultiTriggerPrototype([]func() Trigger) func() Trigger {
	panic("implement me")
}

func (c *MultiTrigger) MultiReached() {
	panic("implement me")
}

func (c *MultiTrigger) WatermarkReceived(watermark time.Time) {
	panic("implement me")
}

func (c *MultiTrigger) KeyReceived(key GroupKey) {
	panic("implement me")
}

func (c *MultiTrigger) Poll() []GroupKey {
	panic("implement me")
}
