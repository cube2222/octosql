package nodes

import (
	"time"

	tbtree "github.com/tidwall/btree"

	. "github.com/cube2222/octosql/execution"
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
	values *tbtree.Generic[*streamJoinSubitem]
}

type streamJoinSubitem struct {
	// Record value
	GroupKey
	// Record event times
	EventTimes []time.Time
}

func (s *StreamJoin) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	panic("implement me")
	// 	type chanMessage struct {
	// 		metadata        bool
	// 		metadataMessage MetadataMessage
	// 		record          RecordBatch
	// 		err             error
	// 	}
	//
	// 	leftMessages := make(chan chanMessage, 10000)
	// 	rightMessages := make(chan chanMessage, 10000)
	//
	// 	go func() {
	// 		if err := s.left.Run(ctx, func(produceCtx ProduceContext, record RecordBatch) error {
	// 			leftMessages <- chanMessage{
	// 				metadata: false,
	// 				record:   record,
	// 			}
	//
	// 			return nil
	// 		}, func(ctx ProduceContext, msg MetadataMessage) error {
	// 			leftMessages <- chanMessage{
	// 				metadata:        true,
	// 				metadataMessage: msg,
	// 			}
	//
	// 			return nil
	// 		}); err != nil {
	// 			leftMessages <- chanMessage{
	// 				err: fmt.Errorf("couldn't run left stream join source: %w", err),
	// 			}
	// 		}
	//
	// 		close(leftMessages)
	// 	}()
	//
	// 	go func() {
	// 		if err := s.right.Run(ctx, func(produceCtx ProduceContext, record RecordBatch) error {
	// 			rightMessages <- chanMessage{
	// 				metadata: false,
	// 				record:   record,
	// 			}
	//
	// 			return nil
	// 		}, func(ctx ProduceContext, msg MetadataMessage) error {
	// 			rightMessages <- chanMessage{
	// 				metadata:        true,
	// 				metadataMessage: msg,
	// 			}
	//
	// 			return nil
	// 		}); err != nil {
	// 			rightMessages <- chanMessage{
	// 				err: fmt.Errorf("couldn't run right stream join source: %w", err),
	// 			}
	// 		}
	//
	// 		close(rightMessages)
	// 	}()
	//
	// 	leftRecords := tbtree.NewGenericOptions[*streamJoinItem](func(a, b *streamJoinItem) bool {
	// 		return CompareValueSlices(a.GroupKey, b.GroupKey)
	// 	}, tbtree.Options{
	// 		NoLocks: true,
	// 	})
	// 	rightRecords := tbtree.NewGenericOptions[*streamJoinItem](func(a, b *streamJoinItem) bool {
	// 		return CompareValueSlices(a.GroupKey, b.GroupKey)
	// 	}, tbtree.Options{
	// 		NoLocks: true,
	// 	})
	//
	// 	var leftDone bool
	//
	// 	var leftWatermark, rightWatermark, minWatermark time.Time
	//
	// 	leftRecordBuffer := NewRecordEventTimeBuffer()
	// 	rightRecordBuffer := NewRecordEventTimeBuffer()
	//
	// 	processRecordsUpTo := func(ctx ExecutionContext, watermark time.Time, oneStreamRemains bool) error {
	// 		if rightRecords != nil {
	// 			if err := leftRecordBuffer.Emit(watermark, func(record RecordBatch) error {
	// 				if err := s.receiveRecord(ctx, produce, leftRecords, rightRecords, true, record, oneStreamRemains); err != nil {
	// 					// TODO: Fix goroutine leak.
	// 					return fmt.Errorf("couldn't process record from left: %w", err)
	// 				}
	// 				return nil
	//
	// 			}); err != nil {
	// 				return err
	// 			}
	// 		}
	//
	// 		if leftRecords != nil {
	// 			if err := rightRecordBuffer.Emit(watermark, func(record RecordBatch) error {
	// 				if err := s.receiveRecord(ctx, produce, rightRecords, leftRecords, false, record, oneStreamRemains); err != nil {
	// 					// TODO: Fix goroutine leak.
	// 					return fmt.Errorf("couldn't process record from right: %w", err)
	// 				}
	// 				return nil
	//
	// 			}); err != nil {
	// 				return err
	// 			}
	// 		}
	//
	// 		return nil
	// 	}
	//
	// receiveLoop:
	// 	for {
	// 		select {
	// 		case msg, ok := <-leftMessages:
	// 			if !ok {
	// 				leftDone = true
	// 				break receiveLoop
	// 			}
	// 			if msg.err != nil {
	// 				return msg.err
	// 			}
	// 			if msg.metadata {
	// 				leftWatermark = msg.metadataMessage.Watermark
	//
	// 				var min time.Time
	// 				if leftWatermark.After(rightWatermark) {
	// 					min = rightWatermark
	// 				} else {
	// 					min = leftWatermark
	// 				}
	// 				if min.After(minWatermark) {
	// 					minWatermark = min
	//
	// 					if err := processRecordsUpTo(ctx, minWatermark, false); err != nil {
	// 						return err
	// 					}
	//
	// 					if err := metaSend(ProduceFromExecutionContext(ctx), MetadataMessage{
	// 						Type:      MetadataMessageTypeWatermark,
	// 						Watermark: minWatermark,
	// 					}); err != nil {
	// 						return fmt.Errorf("couldn't send metadata: %w", err)
	// 					}
	// 				}
	//
	// 				continue
	// 			}
	// 			if msg.record.EventTimes.IsZero() {
	// 				// If the event time is zero, don't buffer, there's no point.
	// 				// There won't be any record with an event time less than zero.
	// 				if err := s.receiveRecord(ctx, produce, leftRecords, rightRecords, true, msg.record, false); err != nil {
	// 					// TODO: Fix goroutine leak.
	// 					return fmt.Errorf("couldn't process record from left: %w", err)
	// 				}
	// 			} else {
	// 				leftRecordBuffer.AddRecord(msg.record)
	// 			}
	// 			// TODO: Add backpressure
	//
	// 		case msg, ok := <-rightMessages:
	// 			if !ok {
	// 				leftDone = false
	// 				break receiveLoop
	// 			}
	// 			if msg.err != nil {
	// 				return msg.err
	// 			}
	// 			if msg.metadata {
	// 				rightWatermark = msg.metadataMessage.Watermark
	//
	// 				var min time.Time
	// 				if rightWatermark.After(leftWatermark) {
	// 					min = leftWatermark
	// 				} else {
	// 					min = rightWatermark
	// 				}
	// 				if min.After(minWatermark) {
	// 					minWatermark = min
	//
	// 					if err := processRecordsUpTo(ctx, minWatermark, false); err != nil {
	// 						return err
	// 					}
	//
	// 					if err := metaSend(ProduceFromExecutionContext(ctx), MetadataMessage{
	// 						Type:      MetadataMessageTypeWatermark,
	// 						Watermark: minWatermark,
	// 					}); err != nil {
	// 						return fmt.Errorf("couldn't send metadata: %w", err)
	// 					}
	// 				}
	//
	// 				continue
	// 			}
	// 			if msg.record.EventTimes.IsZero() {
	// 				// If the event time is zero, don't buffer, there's no point.
	// 				// There won't be any record with an event time less than zero.
	// 				if err := s.receiveRecord(ctx, produce, rightRecords, leftRecords, false, msg.record, false); err != nil {
	// 					// TODO: Fix goroutine leak.
	// 					return fmt.Errorf("couldn't process record from right: %w", err)
	// 				}
	// 			} else {
	// 				rightRecordBuffer.AddRecord(msg.record)
	// 			}
	// 			// TODO: Add backpressure
	// 		}
	// 	}
	//
	// 	var openChannel chan chanMessage
	// 	var myRecordBuffer, otherRecordBuffer *RecordEventTimeBuffer
	// 	var myRecords, otherRecords *tbtree.Generic[*streamJoinItem]
	// 	oneStreamRemains := false
	// 	if !leftDone {
	// 		openChannel = leftMessages
	// 		myRecords = leftRecords
	// 		myRecordBuffer = leftRecordBuffer
	// 		minWatermark = leftWatermark
	// 		otherRecords = rightRecords
	// 		otherRecordBuffer = rightRecordBuffer
	// 	} else {
	// 		openChannel = rightMessages
	// 		myRecords = rightRecords
	// 		myRecordBuffer = rightRecordBuffer
	// 		minWatermark = rightWatermark
	// 		otherRecords = leftRecords
	// 		otherRecordBuffer = leftRecordBuffer
	// 	}
	//
	// 	if err := processRecordsUpTo(ctx, minWatermark, true); err != nil {
	// 		return err
	// 	}
	//
	// 	markOneStreamRemains := func() {
	// 		oneStreamRemains = true
	// 		// We won't be using our tree anymore, let the GC take it.
	// 		myRecords = nil
	//
	// 		if !leftDone {
	// 			leftRecords = nil
	// 		} else {
	// 			rightRecords = nil
	// 		}
	// 	}
	// 	if otherRecordBuffer.Empty() {
	// 		markOneStreamRemains()
	// 	}
	//
	// 	for msg := range openChannel {
	// 		if msg.err != nil {
	// 			return msg.err
	// 		}
	// 		if msg.metadata {
	// 			if err := processRecordsUpTo(ctx, msg.metadataMessage.Watermark, oneStreamRemains); err != nil {
	// 				return err
	// 			}
	//
	// 			if otherRecordBuffer.Empty() {
	// 				markOneStreamRemains()
	// 			}
	//
	// 			if err := metaSend(ProduceFromExecutionContext(ctx), msg.metadataMessage); err != nil {
	// 				return fmt.Errorf("couldn't send metadata: %w", err)
	// 			}
	// 			continue
	// 		}
	//
	// 		if msg.record.EventTimes.IsZero() {
	// 			// If the event time is zero, don't buffer, there's no point.
	// 			// There won't be any record with an event time less than zero.
	// 			if err := s.receiveRecord(ctx, produce, myRecords, otherRecords, !leftDone, msg.record, oneStreamRemains); err != nil {
	// 				return fmt.Errorf("couldn't process record: %w", err)
	// 			}
	// 		} else {
	// 			myRecordBuffer.AddRecord(msg.record)
	// 		}
	// 	}
	//
	// 	if err := processRecordsUpTo(ctx, WatermarkMaxValue, oneStreamRemains); err != nil {
	// 		return err
	// 	}
	//
	// 	return nil
}

func (s *StreamJoin) receiveRecord(ctx ExecutionContext, produce ProduceFn, myRecords, otherRecords *tbtree.Generic[*streamJoinItem], amLeft bool, record RecordBatch, oneStreamRemains bool) error {
	panic("implement me")
	// ctx = ctx.WithRecord(record)
	//
	// var keyExprs []Expression
	// if amLeft {
	// 	keyExprs = s.keyExprsLeft
	// } else {
	// 	keyExprs = s.keyExprsRight
	// }
	//
	// key := make(GroupKey, len(keyExprs))
	// for i, expr := range keyExprs {
	// 	value, err := expr.Evaluate(ctx)
	// 	if err != nil {
	// 		return fmt.Errorf("couldn't evaluate %d stream join key expression: %w", i, err)
	// 	}
	// 	key[i] = value
	// }
	//
	// if !oneStreamRemains {
	// 	// Update count in my record tree
	// 	// If only one stream remains, we won't be using it anymore, so we don't need to update it.
	// 	itemTyped, ok := myRecords.Get(&streamJoinItem{GroupKey: key})
	//
	// 	if !ok {
	// 		itemTyped = &streamJoinItem{GroupKey: key, values: tbtree.NewGenericOptions(func(a, b *streamJoinSubitem) bool {
	// 			return CompareValueSlices(a.GroupKey, b.GroupKey)
	// 		}, tbtree.Options{NoLocks: true})}
	// 		myRecords.Set(itemTyped)
	// 	}
	//
	// 	{
	// 		subitemTyped, ok := itemTyped.values.Get(&streamJoinSubitem{GroupKey: record.Values})
	//
	// 		if !ok {
	// 			subitemTyped = &streamJoinSubitem{GroupKey: record.Values}
	// 			itemTyped.values.Set(subitemTyped)
	// 		}
	// 		if !record.Retractions {
	// 			subitemTyped.EventTimes = append(subitemTyped.EventTimes, record.EventTimes)
	// 		} else {
	// 			// TODO: This should delete the matching event time.
	// 			subitemTyped.EventTimes = subitemTyped.EventTimes[1:]
	// 		}
	// 		if len(subitemTyped.EventTimes) == 0 {
	// 			itemTyped.values.Delete(subitemTyped)
	// 		}
	// 	}
	//
	// 	if itemTyped.values.Len() == 0 {
	// 		myRecords.Delete(itemTyped)
	// 	}
	// }
	//
	// // Trigger with all matching records from other record tree
	// {
	// 	itemTyped, ok := otherRecords.Get(&streamJoinItem{GroupKey: key})
	//
	// 	if !ok {
	// 		// Nothing to trigger
	// 		return nil
	// 	}
	//
	// 	var outErr error
	// 	itemTyped.values.Scan(func(subitemTyped *streamJoinSubitem) bool {
	// 		for i := 0; i < len(subitemTyped.EventTimes); i++ {
	// 			outputValues := make([]octosql.Value, len(record.Values)+len(subitemTyped.GroupKey))
	//
	// 			eventTime := record.EventTimes
	// 			if subitemTyped.EventTimes[i].After(eventTime) {
	// 				eventTime = subitemTyped.EventTimes[i]
	// 			}
	// 			// TODO: We probably also want the event time in the columns to be equal to this. Think about this.
	// 			// TODO: This should be the pairwise maximum of the event times, not the overall maximum.
	//
	// 			if amLeft {
	// 				copy(outputValues, record.Values)
	// 				copy(outputValues[len(record.Values):], subitemTyped.GroupKey)
	// 			} else {
	// 				copy(outputValues, subitemTyped.GroupKey)
	// 				copy(outputValues[len(subitemTyped.GroupKey):], record.Values)
	// 			}
	//
	// 			if err := produce(ProduceFromExecutionContext(ctx), NewRecordBatch(outputValues, record.Retractions, eventTime)); err != nil {
	// 				outErr = fmt.Errorf("couldn't produce: %w", err)
	// 				return false
	// 			}
	// 		}
	//
	// 		return true
	// 	})
	// 	if outErr != nil {
	// 		return outErr
	// 	}
	// }
	//
	// return nil
}
