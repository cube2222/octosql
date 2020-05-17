package execution

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/stretchr/testify/assert"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution/aggregates"
	"github.com/cube2222/octosql/execution/trigger"
	"github.com/cube2222/octosql/storage"
)

func TestExample(t *testing.T) {
	ctx := context.Background()
	stateStorage := storage.GetTestStorage(t)
	streamID := GetRawStreamID()

	groupBy := &GroupByStream{
		prefixes:             [][]byte{[]byte("whatever")},
		inputFields:          []octosql.VariableName{"value"},
		eventTimeField:       "event_time",
		outputEventTimeField: "event_time",
		aggregates:           []Aggregate{aggregates.NewSumAggregate()},
		outputFieldNames:     []octosql.VariableName{"value_sum"},
		streamID:             streamID,
	}
	processFunc := &ProcessByKey{
		eventTimeField:  octosql.NewVariableName("event_time"),
		trigger:         trigger.NewWatermarkTrigger(),
		keyExpressions:  [][]Expression{{NewVariable("key"), NewVariable("event_time")}},
		processFunction: groupBy,
		variables:       octosql.NoVariables(),
	}

	prefixedStateStorage := stateStorage.WithPrefix(streamID.AsPrefix())

	now := time.Now().UTC()

	// Next when no record available
	{
		tx := prefixedStateStorage.BeginTransaction()
		rec, err := processFunc.Next(ctx, tx)
		assert.Nil(t, rec)
		assert.Error(t, GetErrWaitForChanges(err))
		assert.NoError(t, tx.Commit())
	}
	// Get watermark before any actions
	{
		tx := prefixedStateStorage.BeginTransaction()
		watermark, err := processFunc.GetWatermark(ctx, tx)
		assert.Equal(t, time.Time{}, watermark)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}
	// Set watermark with no records pending
	{
		tx := prefixedStateStorage.BeginTransaction()
		err := processFunc.UpdateWatermark(ctx, tx, now)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}
	// Get watermark before watermark apply
	{
		tx := prefixedStateStorage.BeginTransaction()
		watermark, err := processFunc.GetWatermark(ctx, tx)
		assert.Equal(t, time.Time{}, watermark)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}
	// Trigger keys and get next when no record pending.
	// This however will update the watermark behind the scenes.
	{
		tx := prefixedStateStorage.BeginTransaction()

		// Puts the watermark into the output queue (because there's nothing to trigger).
		keys, err := processFunc.TriggerKeys(ctx, tx, 1)
		assert.Equal(t, 0, keys)
		assert.NoError(t, err)

		// Reads the output queue, taking out the watermark and applying it.
		rec, err := processFunc.Next(ctx, tx)
		assert.Nil(t, rec)
		assert.Error(t, GetErrWaitForChanges(err))

		assert.NoError(t, tx.Commit())
	}
	// Read watermark with no records pending
	{
		tx := prefixedStateStorage.BeginTransaction()
		watermark, err := processFunc.GetWatermark(ctx, tx)
		assert.Equal(t, now, watermark)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}
	// Add records.
	{
		tx := prefixedStateStorage.BeginTransaction()
		err := processFunc.AddRecord(ctx, tx, 0, NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"key", "event_time", "value"},
			[]interface{}{"test", now.Add(time.Minute), 5},
			WithEventTimeField("event_time"),
		))
		assert.NoError(t, err)
		err = processFunc.AddRecord(ctx, tx, 0, NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"key", "event_time", "value"},
			[]interface{}{"test", now.Add(time.Minute), 3},
			WithEventTimeField("event_time"),
		))
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}
	// Next when no record available
	{
		tx := prefixedStateStorage.BeginTransaction()
		rec, err := processFunc.Next(ctx, tx)
		assert.Nil(t, rec)
		assert.Error(t, GetErrWaitForChanges(err))
		assert.NoError(t, tx.Commit())
	}
	// Set watermark to trigger records.
	{
		tx := prefixedStateStorage.BeginTransaction()
		err := processFunc.UpdateWatermark(ctx, tx, now.Add(2*time.Minute))
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}
	// Get watermark before triggering keys.
	{
		tx := prefixedStateStorage.BeginTransaction()
		watermark, err := processFunc.GetWatermark(ctx, tx)
		assert.Equal(t, now, watermark)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}
	// Next record before triggering keys.
	{
		tx := prefixedStateStorage.BeginTransaction()
		rec, err := processFunc.Next(ctx, tx)
		assert.Nil(t, rec)
		assert.Error(t, GetErrWaitForChanges(err))
		assert.NoError(t, tx.Commit())
	}
	// Trigger keys.
	// This triggers the record under the watermark.
	{
		tx := prefixedStateStorage.BeginTransaction()
		keys, err := processFunc.TriggerKeys(ctx, tx, 1)
		assert.Equal(t, 1, keys)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}
	// Trigger keys. No keys to trigger
	// This puts the watermark into the output queue.
	{
		tx := prefixedStateStorage.BeginTransaction()
		keys, err := processFunc.TriggerKeys(ctx, tx, 1)
		assert.Equal(t, 0, keys)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}
	// The watermark is in the output queue, still not applied.
	{
		tx := prefixedStateStorage.BeginTransaction()
		watermark, err := processFunc.GetWatermark(ctx, tx)
		assert.Equal(t, now, watermark)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}
	// Get record.
	{
		tx := prefixedStateStorage.BeginTransaction()
		rec, err := processFunc.Next(ctx, tx)
		assert.NoError(t, EqualityOfEverythingButIDs(NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"value_sum"},
			[]interface{}{8},
			WithEventTimeField("event_time"), // TODO: Shouldn't be here, bug.
		), rec))
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}
	// The watermark is in the output queue, still not applied.
	{
		tx := prefixedStateStorage.BeginTransaction()
		watermark, err := processFunc.GetWatermark(ctx, tx)
		assert.Equal(t, now, watermark)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}
	// Next with no record in queue, but will apply watermark.
	{
		tx := prefixedStateStorage.BeginTransaction()
		rec, err := processFunc.Next(ctx, tx)
		assert.Nil(t, rec)
		assert.Error(t, GetErrWaitForChanges(err))
		assert.NoError(t, tx.Commit())
	}
	// Read updated watermark.
	{
		tx := prefixedStateStorage.BeginTransaction()
		watermark, err := processFunc.GetWatermark(ctx, tx)
		assert.Equal(t, now.Add(2*time.Minute), watermark)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}
}

var recordElement = &QueueElement{Type: &QueueElement_Record{Record: NewRecordFromSlice(
	[]octosql.VariableName{
		octosql.NewVariableName("age"),
	},
	[]octosql.Value{
		octosql.MakeInt(3),
	},
)}}
var recordElement2 = &QueueElement{Type: &QueueElement_Record{Record: NewRecordFromSlice(
	[]octosql.VariableName{
		octosql.NewVariableName("age"),
	},
	[]octosql.Value{
		octosql.MakeInt(4),
	},
)}}
var watermarkElement = &QueueElement{Type: &QueueElement_Watermark{Watermark: ptypes.TimestampNow()}}
var eosElement = &QueueElement{Type: &QueueElement_EndOfStream{EndOfStream: true}}

func GetElementAssertNoError(t *testing.T, ctx context.Context, queue *OutputQueue) *QueueElement {
	var element QueueElement
	err := queue.Pop(ctx, &element)
	assert.Nil(t, err)
	return &element
}

func TestOutputQueue_Ok(t *testing.T) {
	stateStorage := storage.GetTestStorage(t)
	ctx := context.Background()

	{
		tx := stateStorage.BeginTransaction()
		queue := NewOutputQueue(tx)
		assert.Nil(t, queue.Push(ctx, recordElement))
		assert.Nil(t, queue.Push(ctx, recordElement2))
		assert.Nil(t, tx.Commit())
		tx = stateStorage.BeginTransaction()
		queue = NewOutputQueue(tx)
		assert.Nil(t, queue.Push(ctx, watermarkElement))
		assert.Nil(t, queue.Push(ctx, recordElement))
		assert.Nil(t, queue.Push(ctx, eosElement))
		assert.Nil(t, tx.Commit())
	}
	{
		tx := stateStorage.BeginTransaction()
		queue := NewOutputQueue(tx)
		assert.True(t, proto.Equal(recordElement, GetElementAssertNoError(t, ctx, queue)))
		assert.True(t, proto.Equal(recordElement2, GetElementAssertNoError(t, ctx, queue)))
		assert.Nil(t, tx.Commit())
		tx = stateStorage.BeginTransaction()
		queue = NewOutputQueue(tx)
		assert.True(t, proto.Equal(watermarkElement, GetElementAssertNoError(t, ctx, queue)))
		assert.True(t, proto.Equal(recordElement, GetElementAssertNoError(t, ctx, queue)))
		assert.True(t, proto.Equal(eosElement, GetElementAssertNoError(t, ctx, queue)))
		assert.Nil(t, tx.Commit())
	}
}

func TestOutputQueue_AbortTransaction(t *testing.T) {
	stateStorage := storage.GetTestStorage(t)
	ctx := context.Background()

	{
		tx := stateStorage.BeginTransaction()
		queue := NewOutputQueue(tx)
		assert.Nil(t, queue.Push(ctx, recordElement))
		assert.Nil(t, queue.Push(ctx, recordElement2))
		tx.Abort()
		tx = stateStorage.BeginTransaction()
		queue = NewOutputQueue(tx)
		assert.Nil(t, queue.Push(ctx, recordElement))
		assert.Nil(t, queue.Push(ctx, recordElement2))
		assert.Nil(t, tx.Commit())
		tx = stateStorage.BeginTransaction()
		queue = NewOutputQueue(tx)
		assert.Nil(t, queue.Push(ctx, watermarkElement))
		assert.Nil(t, queue.Push(ctx, recordElement))
		assert.Nil(t, queue.Push(ctx, eosElement))
		assert.Nil(t, tx.Commit())
	}
	{
		tx := stateStorage.BeginTransaction()
		queue := NewOutputQueue(tx)
		assert.True(t, proto.Equal(recordElement, GetElementAssertNoError(t, ctx, queue)))
		assert.True(t, proto.Equal(recordElement2, GetElementAssertNoError(t, ctx, queue)))
		assert.Nil(t, tx.Commit())
		tx = stateStorage.BeginTransaction()
		queue = NewOutputQueue(tx)
		assert.True(t, proto.Equal(watermarkElement, GetElementAssertNoError(t, ctx, queue)))
		assert.True(t, proto.Equal(recordElement, GetElementAssertNoError(t, ctx, queue)))
		assert.True(t, proto.Equal(eosElement, GetElementAssertNoError(t, ctx, queue)))
		tx.Abort()
		tx = stateStorage.BeginTransaction()
		queue = NewOutputQueue(tx)
		assert.True(t, proto.Equal(watermarkElement, GetElementAssertNoError(t, ctx, queue)))
		assert.True(t, proto.Equal(recordElement, GetElementAssertNoError(t, ctx, queue)))
		assert.True(t, proto.Equal(eosElement, GetElementAssertNoError(t, ctx, queue)))
		assert.Nil(t, tx.Commit())
	}
}

func TestOutputQueue_NewTransactionRequired(t *testing.T) {
	stateStorage := storage.GetTestStorage(t)
	ctx := context.Background()

	{
		readTx := stateStorage.BeginTransaction()
		readQueue := NewOutputQueue(readTx)

		{
			writeTx := stateStorage.BeginTransaction()
			writeQueue := NewOutputQueue(writeTx)

			assert.Nil(t, writeQueue.Push(ctx, recordElement))
			assert.Nil(t, writeTx.Commit())
		}

		var element QueueElement
		err := readQueue.Pop(ctx, &element)
		assert.Equal(t, err, ErrNewTransactionRequired)
		readTx.Abort()

		readTx = stateStorage.BeginTransaction()
		readQueue = NewOutputQueue(readTx)
		assert.True(t, proto.Equal(recordElement, GetElementAssertNoError(t, ctx, readQueue)))
		assert.Nil(t, readTx.Commit())
	}
}

func TestOutputQueue_WaitForChanges(t *testing.T) {
	stateStorage := storage.GetTestStorage(t)
	ctx := context.Background()

	{
		readTx := stateStorage.BeginTransaction()
		readQueue := NewOutputQueue(readTx)
		var phantom octosql.Value
		err := readQueue.Pop(ctx, &phantom)
		assert.IsType(t, err, &ErrWaitForChanges{})
		readTx.Abort()

		errWait := GetErrWaitForChanges(err)

		{
			ctx, cancel := context.WithTimeout(ctx, time.Millisecond)
			defer cancel()
			err := errWait.ListenForChanges(ctx)
			assert.Equal(t, err, context.DeadlineExceeded)
		}

		{
			writeTx := stateStorage.BeginTransaction()
			writeQueue := NewOutputQueue(writeTx)

			assert.Nil(t, writeQueue.Push(ctx, recordElement))
			assert.Nil(t, writeTx.Commit())
		}

		{
			tx := stateStorage.BeginTransaction()
			queue := NewOutputQueue(tx)
			assert.True(t, proto.Equal(recordElement, GetElementAssertNoError(t, ctx, queue)))
			assert.Nil(t, tx.Commit())
		}

		{
			ctx, cancel := context.WithTimeout(ctx, time.Second*10)
			defer cancel()
			err := errWait.ListenForChanges(ctx)
			assert.Nil(t, err)
		}
	}
}
