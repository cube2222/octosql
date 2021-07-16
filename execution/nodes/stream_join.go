package nodes

import (
	"fmt"
	"time"

	"github.com/google/btree"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
)

type StreamJoin struct {
	left, right                 Node
	keyExprsLeft, keyExprsRight []Expression
}

func NewStreamJoin(left, right Node, keyExprsLeft, keyExprsRight []Expression) *StreamJoin {
	return &StreamJoin{
		left:          left,
		right:         right,
		keyExprsLeft:  keyExprsLeft,
		keyExprsRight: keyExprsRight,
	}
}

type streamJoinItem struct {
	GroupKey
	// Records for this key
	values *btree.BTree
}

type streamJoinSubitem struct {
	// Record value
	GroupKey
	// Event time
	EventTime time.Time
	// Record count
	count int
}

func (subItem *streamJoinSubitem) Less(than btree.Item) bool {
	thanTyped, ok := than.(*streamJoinSubitem)
	if !ok {
		panic(fmt.Sprintf("invalid *streamJoinSubitem comparison: %T", than))
	}

	// TODO: We could optimize this by using the underlying Compare of GroupKey, instead of doing two-way comparisons.
	if less := subItem.GroupKey.Less(thanTyped.GroupKey); less {
		return less
	} else if greater := thanTyped.GroupKey.Less(subItem.GroupKey); greater {
		return false
	}

	return subItem.EventTime.Before(thanTyped.EventTime)
}

func (s *StreamJoin) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	type chanMessage struct {
		metadata        bool
		metadataMessage MetadataMessage
		record          Record
		err             error
	}

	leftMessages := make(chan chanMessage, 10000)
	rightMessages := make(chan chanMessage, 10000)

	go func() {
		if err := s.left.Run(ctx, func(produceCtx ProduceContext, record Record) error {
			leftMessages <- chanMessage{
				metadata: false,
				record:   record,
			}

			return nil
		}, func(ctx ProduceContext, msg MetadataMessage) error {
			leftMessages <- chanMessage{
				metadata:        true,
				metadataMessage: msg,
			}

			return nil
		}); err != nil {
			leftMessages <- chanMessage{
				err: fmt.Errorf("couldn't run left stream join source: %w", err),
			}
		}

		close(leftMessages)
	}()

	go func() {
		if err := s.right.Run(ctx, func(produceCtx ProduceContext, record Record) error {
			rightMessages <- chanMessage{
				metadata: false,
				record:   record,
			}

			return nil
		}, func(ctx ProduceContext, msg MetadataMessage) error {
			rightMessages <- chanMessage{
				metadata:        true,
				metadataMessage: msg,
			}

			return nil
		}); err != nil {
			rightMessages <- chanMessage{
				err: fmt.Errorf("couldn't run right stream join source: %w", err),
			}
		}

		close(rightMessages)
	}()

	leftRecords := btree.New(BTreeDefaultDegree)
	rightRecords := btree.New(BTreeDefaultDegree)

	var leftDone bool

	var leftWatermark, rightWatermark, minWatermark time.Time

receiveLoop:
	for {
		select {
		case msg, ok := <-leftMessages:
			if !ok {
				leftDone = true
				break receiveLoop
			}
			if msg.err != nil {
				return msg.err
			}
			if msg.metadata {
				leftWatermark = msg.metadataMessage.Watermark

				var min time.Time
				if leftWatermark.After(rightWatermark) {
					min = rightWatermark
				} else {
					min = leftWatermark
				}
				if min.After(minWatermark) {
					minWatermark = min
					if err := metaSend(ProduceFromExecutionContext(ctx), MetadataMessage{
						Type:      MetadataMessageTypeWatermark,
						Watermark: minWatermark,
					}); err != nil {
						return fmt.Errorf("couldn't send metadata: %w", err)
					}
				}

				continue
			}
			if err := s.receiveRecord(ctx, produce, leftRecords, rightRecords, true, msg.record); err != nil {
				// TODO: Fix goroutine leak.
				return fmt.Errorf("couldn't consume record from left: %w", err)
			}
		case msg, ok := <-rightMessages:
			if !ok {
				leftDone = false
				break receiveLoop
			}
			if msg.err != nil {
				return msg.err
			}
			if msg.metadata {
				rightWatermark = msg.metadataMessage.Watermark

				var min time.Time
				if rightWatermark.After(leftWatermark) {
					min = leftWatermark
				} else {
					min = rightWatermark
				}
				if min.After(minWatermark) {
					minWatermark = min
					if err := metaSend(ProduceFromExecutionContext(ctx), MetadataMessage{
						Type:      MetadataMessageTypeWatermark,
						Watermark: minWatermark,
					}); err != nil {
						return fmt.Errorf("couldn't send metadata: %w", err)
					}
				}

				continue
			}
			if err := s.receiveRecord(ctx, produce, rightRecords, leftRecords, false, msg.record); err != nil {
				// TODO: Fix goroutine leak.
				return fmt.Errorf("couldn't consume record from right: %w", err)
			}
		}
	}

	var openChannel chan chanMessage
	if !leftDone {
		openChannel = leftMessages
	} else {
		openChannel = rightMessages
	}

	for msg := range openChannel {
		if msg.err != nil {
			return msg.err
		}
		if msg.metadata {
			if err := metaSend(ProduceFromExecutionContext(ctx), msg.metadataMessage); err != nil {
				return fmt.Errorf("couldn't send metadata: %w", err)
			}
			continue
		}

		if !leftDone {
			if err := s.receiveRecord(ctx, produce, leftRecords, rightRecords, true, msg.record); err != nil {
				// TODO: Fix goroutine leak.
				return fmt.Errorf("couldn't consume record from left: %w", err)
			}
		} else {
			if err := s.receiveRecord(ctx, produce, rightRecords, leftRecords, false, msg.record); err != nil {
				// TODO: Fix goroutine leak.
				return fmt.Errorf("couldn't consume record from left: %w", err)
			}
		}
	}

	return nil
}

func (s *StreamJoin) receiveRecord(ctx ExecutionContext, produce ProduceFn, myRecords, otherRecords *btree.BTree, amLeft bool, record Record) error {
	ctx = ctx.WithRecord(record)

	var keyExprs []Expression
	if amLeft {
		keyExprs = s.keyExprsLeft
	} else {
		keyExprs = s.keyExprsRight
	}

	key := make(GroupKey, len(keyExprs))
	for i, expr := range keyExprs {
		value, err := expr.Evaluate(ctx)
		if err != nil {
			return fmt.Errorf("couldn't evaluate %d stream join key expression: %w", i, err)
		}
		key[i] = value
	}

	// Update count in my record tree
	{
		item := myRecords.Get(key)
		var itemTyped *streamJoinItem

		if item == nil {
			itemTyped = &streamJoinItem{GroupKey: key, values: btree.New(BTreeDefaultDegree)}
			myRecords.ReplaceOrInsert(itemTyped)
		} else {
			var ok bool
			itemTyped, ok = item.(*streamJoinItem)
			if !ok {
				panic(fmt.Sprintf("invalid stream join item: %v", item))
			}
		}

		{
			subitem := itemTyped.values.Get(key)
			var subitemTyped *streamJoinSubitem

			if subitem == nil {
				subitemTyped = &streamJoinSubitem{GroupKey: record.Values, EventTime: record.EventTime}
				itemTyped.values.ReplaceOrInsert(subitemTyped)
			} else {
				var ok bool
				subitemTyped, ok = subitem.(*streamJoinSubitem)
				if !ok {
					panic(fmt.Sprintf("invalid stream join subitem: %v", subitem))
				}
			}
			if !record.Retraction {
				subitemTyped.count++
			} else {
				subitemTyped.count--
			}
			if subitemTyped.count == 0 {
				itemTyped.values.Delete(subitemTyped)
			}
		}

		if itemTyped.values.Len() == 0 {
			myRecords.Delete(itemTyped)
		}
	}

	// Trigger with all matching records from other record tree
	{
		item := otherRecords.Get(key)
		var itemTyped *streamJoinItem

		if item == nil {
			// Nothing to trigger
			return nil
		} else {
			var ok bool
			itemTyped, ok = item.(*streamJoinItem)
			if !ok {
				panic(fmt.Sprintf("invalid stream join item: %v", item))
			}
		}

		var outErr error
		itemTyped.values.Ascend(func(subitem btree.Item) bool {
			subitemTyped, ok := subitem.(*streamJoinSubitem)
			if !ok {
				panic(fmt.Sprintf("invalid stream join subitem: %v", subitem))
			}

			for i := 0; i < subitemTyped.count; i++ {
				outputValues := make([]octosql.Value, len(record.Values)+len(subitemTyped.GroupKey))

				eventTime := record.EventTime
				if subitemTyped.EventTime.After(eventTime) {
					eventTime = subitemTyped.EventTime
				}
				// TODO: We probably also want the event time in the columns to be equal to this. Think about this.

				if amLeft {
					copy(outputValues, record.Values)
					copy(outputValues[len(record.Values):], subitemTyped.GroupKey)
				} else {
					copy(outputValues, subitemTyped.GroupKey)
					copy(outputValues[len(subitemTyped.GroupKey):], record.Values)
				}

				if err := produce(ProduceFromExecutionContext(ctx), NewRecord(outputValues, record.Retraction, eventTime)); err != nil {
					outErr = fmt.Errorf("couldn't produce: %w", err)
					return false
				}
			}

			return true
		})
		if outErr != nil {
			return outErr
		}
	}

	return nil
}
