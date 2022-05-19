package execution

import (
	"fmt"
	"time"

	"github.com/google/btree"
)

type RecordEventTimeBuffer struct {
	tree *btree.BTree
}

func NewRecordEventTimeBuffer() *RecordEventTimeBuffer {
	return &RecordEventTimeBuffer{
		tree: btree.New(BTreeDefaultDegree),
	}
}

type recordEventTimeBufferItem struct {
	EventTime time.Time
	Records   []Record
}

func (e *recordEventTimeBufferItem) Less(than btree.Item) bool {
	thanTyped, ok := than.(*recordEventTimeBufferItem)
	if !ok {
		panic(fmt.Sprintf("invalid event time key comparison: %T", than))
	}

	return e.EventTime.Before(thanTyped.EventTime)
}

func (b *RecordEventTimeBuffer) AddRecord(record Record) {
	item := b.tree.Get(&recordEventTimeBufferItem{EventTime: record.EventTime})
	var itemTyped *recordEventTimeBufferItem

	if item == nil {
		itemTyped = &recordEventTimeBufferItem{EventTime: record.EventTime}
		b.tree.ReplaceOrInsert(itemTyped)
	} else {
		var ok bool
		itemTyped, ok = item.(*recordEventTimeBufferItem)
		if !ok {
			panic(fmt.Sprintf("invalid event time buffer item: %v", item))
		}
	}

	itemTyped.Records = append(itemTyped.Records, record)
}

func (b *RecordEventTimeBuffer) Emit(watermark time.Time, produce func(record Record) error) error {
	min := b.tree.Min()
	for min != nil && !min.(*recordEventTimeBufferItem).EventTime.After(watermark) {
		b.tree.DeleteMin()
		for _, record := range min.(*recordEventTimeBufferItem).Records {
			if err := produce(record); err != nil {
				return err
			}
		}
		min = b.tree.Min()
	}
	return nil
}

func (b *RecordEventTimeBuffer) Empty() bool {
	return b.tree.Len() == 0
}
