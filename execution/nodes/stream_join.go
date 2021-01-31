package nodes

import (
	"fmt"

	"github.com/google/btree"

	"github.com/cube2222/octosql"
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
	values *btree.BTree
}

type streamJoinSubitem struct {
	// Record value
	GroupKey
	// Record count
	count int
}

func (s *StreamJoin) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	type chanMessage struct {
		metadata        bool
		metadataMessage MetadataMessage
		record          Record
	}

	leftMessages := make(chan chanMessage, 10000)
	rightMessages := make(chan chanMessage, 10000)

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
		return fmt.Errorf("couldn't run left stream join source: %w", err)
	}

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
		return fmt.Errorf("couldn't run right stream join source: %w", err)
	}

	leftRecords := btree.New(BTreeDefaultDegree)
	rightRecords := btree.New(BTreeDefaultDegree)

	var leftDone bool

receiveLoop:
	for {
		select {
		case msg, ok := <-leftMessages:
			if !ok {
				leftDone = true
				break receiveLoop
			}
			if msg.metadata {
				if err := metaSend(ProduceFromExecutionContext(ctx), msg.metadataMessage); err != nil {
					return fmt.Errorf("couldn't send metadata: %w", err)
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
			if msg.metadata {
				if err := metaSend(ProduceFromExecutionContext(ctx), msg.metadataMessage); err != nil {
					return fmt.Errorf("couldn't send metadata: %w", err)
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
			return fmt.Errorf("couldn't evaluate %d stream join left key expression: %w", i, err)
		}
		key[i] = value
	}

	// Update count in my record tree
	{
		item := myRecords.Get(key)
		var itemTyped *streamJoinItem

		if item == nil {
			itemTyped = &streamJoinItem{GroupKey: key}
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

				if amLeft {
					copy(outputValues, record.Values)
					copy(outputValues[len(record.Values):], subitemTyped.GroupKey)
				} else {
					copy(outputValues, subitemTyped.GroupKey)
					copy(outputValues[len(subitemTyped.GroupKey):], record.Values)
				}

				if err := produce(ProduceFromExecutionContext(ctx), NewRecord(outputValues, record.Retraction)); err != nil {
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
