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
	// Record event times
	EventTimes []time.Time
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

	leftRecordBuffer := NewRecordEventTimeBuffer()
	rightRecordBuffer := NewRecordEventTimeBuffer()

	processRecordsUpTo := func(ctx ExecutionContext, watermark time.Time) error {
		if err := leftRecordBuffer.Emit(watermark, func(record Record) error {
			if err := s.receiveRecord(ctx, produce, leftRecords, rightRecords, true, record, false); err != nil {
				// TODO: Fix goroutine leak.
				return fmt.Errorf("couldn't process record from left: %w", err)
			}
			return nil

		}); err != nil {
			return err
		}

		if err := rightRecordBuffer.Emit(watermark, func(record Record) error {
			if err := s.receiveRecord(ctx, produce, rightRecords, leftRecords, false, record, false); err != nil {
				// TODO: Fix goroutine leak.
				return fmt.Errorf("couldn't process record from right: %w", err)
			}
			return nil

		}); err != nil {
			return err
		}

		return nil
	}

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

					if err := processRecordsUpTo(ctx, minWatermark); err != nil {
						return err
					}

					if err := metaSend(ProduceFromExecutionContext(ctx), MetadataMessage{
						Type:      MetadataMessageTypeWatermark,
						Watermark: minWatermark,
					}); err != nil {
						return fmt.Errorf("couldn't send metadata: %w", err)
					}
				}

				continue
			}
			if msg.record.EventTime.IsZero() {
				// If the event time is zero, don't buffer, there's no point.
				// There won't be any record with an event time less than zero.
				if err := s.receiveRecord(ctx, produce, leftRecords, rightRecords, true, msg.record, false); err != nil {
					// TODO: Fix goroutine leak.
					return fmt.Errorf("couldn't process record from left: %w", err)
				}
			} else {
				leftRecordBuffer.AddRecord(msg.record)
			}
			// TODO: Add backpressure

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

					if err := processRecordsUpTo(ctx, minWatermark); err != nil {
						return err
					}

					if err := metaSend(ProduceFromExecutionContext(ctx), MetadataMessage{
						Type:      MetadataMessageTypeWatermark,
						Watermark: minWatermark,
					}); err != nil {
						return fmt.Errorf("couldn't send metadata: %w", err)
					}
				}

				continue
			}
			if msg.record.EventTime.IsZero() {
				// If the event time is zero, don't buffer, there's no point.
				// There won't be any record with an event time less than zero.
				if err := s.receiveRecord(ctx, produce, rightRecords, leftRecords, false, msg.record, false); err != nil {
					// TODO: Fix goroutine leak.
					return fmt.Errorf("couldn't process record from right: %w", err)
				}
			} else {
				rightRecordBuffer.AddRecord(msg.record)
			}
			// TODO: Add backpressure
		}
	}

	var openChannel chan chanMessage
	var recordBuffer *RecordEventTimeBuffer
	var otherRecords *btree.BTree
	if !leftDone {
		openChannel = leftMessages
		recordBuffer = leftRecordBuffer
		minWatermark = leftWatermark
		otherRecords = rightRecords

		// We won't be using our tree anymore, let the GC take it.
		leftRecords = nil
	} else {
		openChannel = rightMessages
		recordBuffer = rightRecordBuffer
		minWatermark = rightWatermark
		otherRecords = leftRecords

		// We won't be using our tree anymore, let the GC take it.
		rightRecords = nil
	}

	if err := processRecordsUpTo(ctx, minWatermark); err != nil {
		return err
	}

	for msg := range openChannel {
		if msg.err != nil {
			return msg.err
		}
		if msg.metadata {
			if err := processRecordsUpTo(ctx, msg.metadataMessage.Watermark); err != nil {
				return err
			}

			if err := metaSend(ProduceFromExecutionContext(ctx), msg.metadataMessage); err != nil {
				return fmt.Errorf("couldn't send metadata: %w", err)
			}
			continue
		}

		if msg.record.EventTime.IsZero() {
			// If the event time is zero, don't buffer, there's no point.
			// There won't be any record with an event time less than zero.
			if err := s.receiveRecord(ctx, produce, nil, otherRecords, !leftDone, msg.record, true); err != nil {
				// TODO: Fix goroutine leak.
				return fmt.Errorf("couldn't process record: %w", err)
			}
		} else {
			recordBuffer.AddRecord(msg.record)
		}
	}

	if err := processRecordsUpTo(ctx, WatermarkMaxValue); err != nil {
		return err
	}

	return nil
}

func (s *StreamJoin) receiveRecord(ctx ExecutionContext, produce ProduceFn, myRecords, otherRecords *btree.BTree, amLeft bool, record Record, oneStreamRemains bool) error {
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

	if !oneStreamRemains {
		// Update count in my record tree
		// If only one stream remains, we won't be using it anymore, so we don't need to update it.
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
			subitem := itemTyped.values.Get(&streamJoinSubitem{GroupKey: record.Values})
			var subitemTyped *streamJoinSubitem

			if subitem == nil {
				subitemTyped = &streamJoinSubitem{GroupKey: record.Values}
				itemTyped.values.ReplaceOrInsert(subitemTyped)
			} else {
				var ok bool
				subitemTyped, ok = subitem.(*streamJoinSubitem)
				if !ok {
					panic(fmt.Sprintf("invalid stream join subitem: %v", subitem))
				}
			}
			if !record.Retraction {
				subitemTyped.EventTimes = append(subitemTyped.EventTimes, record.EventTime)
			} else {
				subitemTyped.EventTimes = subitemTyped.EventTimes[1:]
			}
			if len(subitemTyped.EventTimes) == 0 {
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

			for i := 0; i < len(subitemTyped.EventTimes); i++ {
				outputValues := make([]octosql.Value, len(record.Values)+len(subitemTyped.GroupKey))

				eventTime := record.EventTime
				if subitemTyped.EventTimes[i].After(eventTime) {
					eventTime = subitemTyped.EventTimes[i]
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
