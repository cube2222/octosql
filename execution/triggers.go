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

func NewTriggerPrototype(triggerAfter uint) func() Trigger {
	return func() Trigger {
		return &CountingTrigger{
			triggerAfter:       triggerAfter,
			counts:             btree.New(DefaultBTreeDegree),
			endOfStreamReached: false,
			toTrigger:          []GroupKey{},
		}
	}
}

type countingTriggerItem struct {
	Key   GroupKey
	Count uint
}

func (c *countingTriggerItem) Less(than btree.Item) bool {
	thanTyped, ok := than.(*countingTriggerItem)
	if !ok {
		panic(fmt.Sprintf("invalid key comparison: %T", than))
	}

	return c.Key.Less(thanTyped.Key)
}

func (c *CountingTrigger) EndOfStreamReached() {
	c.endOfStreamReached = true
}

func (c *CountingTrigger) WatermarkReceived(watermark time.Time) {}

func (c *CountingTrigger) KeyReceived(key GroupKey) {
	item := c.counts.Get(&countingTriggerItem{Key: key})
	var itemTyped *countingTriggerItem

	if item == nil {
		itemTyped = &countingTriggerItem{Key: key}
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
		c.toTrigger = append(c.toTrigger, itemTyped.Key)
		c.counts.Delete(itemTyped)
	}
}

// The returned slice will be made invalid after following operations on the trigger.
func (c *CountingTrigger) Poll() []GroupKey {
	output := c.toTrigger
	c.toTrigger = c.toTrigger[:0]
	return output
}
