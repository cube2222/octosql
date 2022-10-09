package nodes

import (
	tbtree "github.com/tidwall/btree"

	. "github.com/cube2222/octosql/execution"
)

type OuterJoin struct {
	left, right                     Node
	leftFieldCount, rightFieldCount int
	keyExprsLeft, keyExprsRight     []Expression
	isOuterLeft, isOuterRight       bool
}

func NewOuterJoin(left, right Node, leftFieldCount, rightFieldCount int, keyExprsLeft, keyExprsRight []Expression, isOuterLeft, isOuterRight bool) *OuterJoin {
	return &OuterJoin{
		left:            left,
		right:           right,
		leftFieldCount:  leftFieldCount,
		rightFieldCount: rightFieldCount,
		keyExprsLeft:    keyExprsLeft,
		keyExprsRight:   keyExprsRight,
		isOuterLeft:     isOuterLeft,
		isOuterRight:    isOuterRight,
	}
}

func (s *OuterJoin) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
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
	// 	processRecordsUpTo := func(ctx ExecutionContext, watermark time.Time) error {
	// 		if rightRecords != nil {
	// 			if err := leftRecordBuffer.Emit(watermark, func(record RecordBatch) error {
	// 				if err := s.receiveRecord(ctx, produce, leftRecords, rightRecords, true, record); err != nil {
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
	// 				if err := s.receiveRecord(ctx, produce, rightRecords, leftRecords, false, record); err != nil {
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
	// 					if err := processRecordsUpTo(ctx, minWatermark); err != nil {
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
	// 				if err := s.receiveRecord(ctx, produce, leftRecords, rightRecords, true, msg.record); err != nil {
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
	// 					if err := processRecordsUpTo(ctx, minWatermark); err != nil {
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
	// 				if err := s.receiveRecord(ctx, produce, rightRecords, leftRecords, false, msg.record); err != nil {
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
	// 	var myRecordBuffer *RecordEventTimeBuffer
	// 	var myRecords, otherRecords *tbtree.Generic[*streamJoinItem]
	// 	if !leftDone {
	// 		openChannel = leftMessages
	// 		myRecords = leftRecords
	// 		myRecordBuffer = leftRecordBuffer
	// 		minWatermark = leftWatermark
	// 		otherRecords = rightRecords
	// 	} else {
	// 		openChannel = rightMessages
	// 		myRecords = rightRecords
	// 		myRecordBuffer = rightRecordBuffer
	// 		minWatermark = rightWatermark
	// 		otherRecords = leftRecords
	// 	}
	//
	// 	if err := processRecordsUpTo(ctx, minWatermark); err != nil {
	// 		return err
	// 	}
	//
	// 	for msg := range openChannel {
	// 		if msg.err != nil {
	// 			return msg.err
	// 		}
	// 		if msg.metadata {
	// 			if err := processRecordsUpTo(ctx, msg.metadataMessage.Watermark); err != nil {
	// 				return err
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
	// 			if err := s.receiveRecord(ctx, produce, myRecords, otherRecords, !leftDone, msg.record); err != nil {
	// 				return fmt.Errorf("couldn't process record: %w", err)
	// 			}
	// 		} else {
	// 			myRecordBuffer.AddRecord(msg.record)
	// 		}
	// 	}
	//
	// 	if err := processRecordsUpTo(ctx, WatermarkMaxValue); err != nil {
	// 		return err
	// 	}
	//
	// 	return nil
}

func (s *OuterJoin) receiveRecord(ctx ExecutionContext, produce ProduceFn, myRecords, otherRecords *tbtree.Generic[*streamJoinItem], amLeft bool, record RecordBatch) error {
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
	// firstRecordForThatKeyOnThisSide := false
	// lastRetractionForThatKeyOnThisSide := false
	// {
	// 	// Update count in my record tree
	// 	itemTyped, ok := myRecords.Get(&streamJoinItem{GroupKey: key})
	//
	// 	if !ok {
	// 		itemTyped = &streamJoinItem{GroupKey: key, values: tbtree.NewGenericOptions(func(a, b *streamJoinSubitem) bool {
	// 			return CompareValueSlices(a.GroupKey, b.GroupKey)
	// 		}, tbtree.Options{NoLocks: true})}
	// 		myRecords.Set(itemTyped)
	// 		firstRecordForThatKeyOnThisSide = true
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
	// 		lastRetractionForThatKeyOnThisSide = true
	// 	}
	// }
	//
	// // Trigger with all matching records from other record tree
	// {
	// 	itemTyped, ok := otherRecords.Get(&streamJoinItem{GroupKey: key})
	//
	// 	if !ok || itemTyped.values.Len() == 0 {
	// 		if s.isOuterLeft && amLeft {
	// 			// We're an outer join, so trigger record with nulls on other side.
	// 			outputValues := make([]octosql.Value, s.leftFieldCount+s.rightFieldCount)
	// 			copy(outputValues, record.Values)
	// 			if err := produce(ProduceFromExecutionContext(ctx), NewRecordBatch(outputValues, record.Retractions, record.EventTimes)); err != nil {
	// 				return fmt.Errorf("couldn't produce: %w", err)
	// 			}
	// 			return nil
	// 		} else if s.isOuterRight && !amLeft {
	// 			// We're an outer join, so trigger record with nulls on other side.
	// 			outputValues := make([]octosql.Value, s.leftFieldCount+s.rightFieldCount)
	// 			copy(outputValues[s.leftFieldCount:], record.Values)
	// 			if err := produce(ProduceFromExecutionContext(ctx), NewRecordBatch(outputValues, record.Retractions, record.EventTimes)); err != nil {
	// 				return fmt.Errorf("couldn't produce: %w", err)
	// 			}
	// 			return nil
	// 		} else {
	// 			// Nothing to trigger
	// 			return nil
	// 		}
	// 	}
	//
	// 	if firstRecordForThatKeyOnThisSide && ((s.isOuterLeft && !amLeft) || (s.isOuterRight && amLeft)) {
	// 		// We need to send retractions for previous null records on the other side.
	// 		var outErr error
	// 		itemTyped.values.Scan(func(subitemTyped *streamJoinSubitem) bool {
	// 			for i := 0; i < len(subitemTyped.EventTimes); i++ {
	// 				outputValues := make([]octosql.Value, len(record.Values)+len(subitemTyped.GroupKey))
	// 				if amLeft {
	// 					copy(outputValues[len(record.Values):], subitemTyped.GroupKey)
	// 				} else {
	// 					copy(outputValues, subitemTyped.GroupKey)
	// 				}
	//
	// 				if err := produce(ProduceFromExecutionContext(ctx), NewRecordBatch(outputValues, true, subitemTyped.EventTimes[i])); err != nil {
	// 					outErr = fmt.Errorf("couldn't produce: %w", err)
	// 					return false
	// 				}
	// 			}
	//
	// 			return true
	// 		})
	// 		if outErr != nil {
	// 			return outErr
	// 		}
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
	//
	// 	if lastRetractionForThatKeyOnThisSide && ((s.isOuterLeft && !amLeft) || (s.isOuterRight && amLeft)) {
	// 		// We need to send null records on the other side.
	// 		var outErr error
	// 		itemTyped.values.Scan(func(subitemTyped *streamJoinSubitem) bool {
	// 			for i := 0; i < len(subitemTyped.EventTimes); i++ {
	// 				outputValues := make([]octosql.Value, len(record.Values)+len(subitemTyped.GroupKey))
	// 				if amLeft {
	// 					copy(outputValues[len(record.Values):], subitemTyped.GroupKey)
	// 				} else {
	// 					copy(outputValues, subitemTyped.GroupKey)
	// 				}
	//
	// 				if err := produce(ProduceFromExecutionContext(ctx), NewRecordBatch(outputValues, false, subitemTyped.EventTimes[i])); err != nil {
	// 					outErr = fmt.Errorf("couldn't produce: %w", err)
	// 					return false
	// 				}
	// 			}
	//
	// 			return true
	// 		})
	// 		if outErr != nil {
	// 			return outErr
	// 		}
	// 	}
	// }
	//
	// return nil
}
