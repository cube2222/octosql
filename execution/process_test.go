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

func TestProcessByKey_GarbageCollection(t *testing.T) {
	ctx := context.Background()
	stateStorage := storage.GetTestStorage(t)
	streamID := GetRawStreamID()

	groupBy := &GroupByStream{
		prefixes:             [][]byte{[]byte("whatever"), []byte("still_whatever")},
		inputFields:          []octosql.VariableName{"event_time", "livesleft"},
		eventTimeField:       "event_time",
		outputEventTimeField: "event_time",
		aggregates:           []Aggregate{aggregates.NewKeyAggregate(), aggregates.NewSumAggregate()},
		outputFieldNames:     []octosql.VariableName{"event_time", "livesleft_sum"},
		streamID:             streamID,
	}
	processFunc := &ProcessByKey{
		eventTimeField:  octosql.NewVariableName("event_time"),
		trigger:         trigger.NewWatermarkTrigger(),
		keyExpressions:  [][]Expression{{NewVariable("event_time")}},
		processFunction: groupBy,
		variables:       octosql.NoVariables(),
	}

	prefixedStateStorage := stateStorage.WithPrefix(streamID.AsPrefix())

	now := time.Now().UTC()

	fields := []octosql.VariableName{"cat", "livesleft", "event_time"}

	firstWindow := now
	record11 := NewRecordFromSliceWithNormalize(fields, []interface{}{"Buster", 100, firstWindow}, WithEventTimeField(octosql.NewVariableName("event_time")))
	record12 := NewRecordFromSliceWithNormalize(fields, []interface{}{"Precious", 101, firstWindow}, WithEventTimeField(octosql.NewVariableName("event_time")))
	record13 := NewRecordFromSliceWithNormalize(fields, []interface{}{"Nala", 102, firstWindow}, WithEventTimeField(octosql.NewVariableName("event_time")))
	record14 := NewRecordFromSliceWithNormalize(fields, []interface{}{"Tiger", 103, firstWindow}, WithEventTimeField(octosql.NewVariableName("event_time")))
	record15 := NewRecordFromSliceWithNormalize(fields, []interface{}{"Lucy", 104, firstWindow}, WithEventTimeField(octosql.NewVariableName("event_time")))

	secondWindow := now.Add(time.Minute * 5)
	record21 := NewRecordFromSliceWithNormalize(fields, []interface{}{"Buster", 9, secondWindow}, WithEventTimeField(octosql.NewVariableName("event_time")))
	record22 := NewRecordFromSliceWithNormalize(fields, []interface{}{"Nala", 6, secondWindow}, WithEventTimeField(octosql.NewVariableName("event_time")))
	record23 := NewRecordFromSliceWithNormalize(fields, []interface{}{"Lucy", 4, secondWindow}, WithEventTimeField(octosql.NewVariableName("event_time")))
	record24 := NewRecordFromSliceWithNormalize(fields, []interface{}{"Tiger", 5, secondWindow}, WithEventTimeField(octosql.NewVariableName("event_time")))

	thirdWindow := now.Add(time.Minute * 10)
	record31 := NewRecordFromSliceWithNormalize(fields, []interface{}{"Buster", 8, thirdWindow}, WithEventTimeField(octosql.NewVariableName("event_time")))
	record32 := NewRecordFromSliceWithNormalize(fields, []interface{}{"Nala", 6, thirdWindow}, WithEventTimeField(octosql.NewVariableName("event_time")))
	record33 := NewRecordFromSliceWithNormalize(fields, []interface{}{"Lucy", 1, thirdWindow}, WithEventTimeField(octosql.NewVariableName("event_time")))
	record34 := NewRecordFromSliceWithNormalize(fields, []interface{}{"Tiger", 2, thirdWindow}, WithEventTimeField(octosql.NewVariableName("event_time")))

	// Garbage collector stuff
	gbCtx, cancel := context.WithCancel(ctx)
	processFunc.garbageCollectorCtxCancel = cancel
	processFunc.garbageCollectorCloseErrChan = make(chan error, 1)
	go func() {
		_ = processFunc.RunGarbageCollector(gbCtx, prefixedStateStorage, 360, 1) // Check every 1 second, boundary: watermark - 6min
	}()

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
		err := processFunc.UpdateWatermark(ctx, tx, now.Add(5*time.Minute)) // GC: No deletions
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
		assert.Equal(t, now.Add(5*time.Minute), watermark)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}
	// Add records. (all from first window, 2 from second window, 2 from third window)
	{
		tx := prefixedStateStorage.BeginTransaction()

		err := processFunc.AddRecord(ctx, tx, 0, record11)
		assert.NoError(t, err)
		err = processFunc.AddRecord(ctx, tx, 0, record12)
		assert.NoError(t, err)
		err = processFunc.AddRecord(ctx, tx, 0, record13)
		assert.NoError(t, err)
		err = processFunc.AddRecord(ctx, tx, 0, record14)
		assert.NoError(t, err)
		err = processFunc.AddRecord(ctx, tx, 0, record15)
		assert.NoError(t, err)

		err = processFunc.AddRecord(ctx, tx, 0, record21)
		assert.NoError(t, err)
		err = processFunc.AddRecord(ctx, tx, 0, record22)
		assert.NoError(t, err)

		err = processFunc.AddRecord(ctx, tx, 0, record31)
		assert.NoError(t, err)
		err = processFunc.AddRecord(ctx, tx, 0, record32)
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
	// Set watermark to trigger records
	{
		tx := prefixedStateStorage.BeginTransaction()
		err := processFunc.UpdateWatermark(ctx, tx, now.Add(10*time.Minute)) // GC: First window deleted (10 - 6 = 4 > firstWindow)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}
	// Get watermark before triggering keys
	{
		tx := prefixedStateStorage.BeginTransaction()
		watermark, err := processFunc.GetWatermark(ctx, tx)
		assert.Equal(t, now.Add(5*time.Minute), watermark)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}
	// Trigger keys.
	// This triggers the records under the watermark.
	{
		tx := prefixedStateStorage.BeginTransaction()
		keys, err := processFunc.TriggerKeys(ctx, tx, 3)
		assert.Equal(t, 2, keys) // OMG this is actually happening (!!!)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}
	// Trigger keys. No keys to trigger
	// This puts the watermark into the output queue.
	{
		tx := prefixedStateStorage.BeginTransaction()
		keys, err := processFunc.TriggerKeys(ctx, tx, 3)
		assert.Equal(t, 0, keys)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}

	time.Sleep(2 * time.Second) // Now the garbage collector should delete records from first window

	// Get record.
	{
		tx := prefixedStateStorage.BeginTransaction()
		rec, err := processFunc.Next(ctx, tx)
		assert.NoError(t, EqualityOfEverythingButIDs(NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"event_time", "livesleft_sum"},
			[]interface{}{secondWindow, 15},
			WithEventTimeField("event_time"),
		), rec))
		rec, err = processFunc.Next(ctx, tx)
		assert.NoError(t, EqualityOfEverythingButIDs(NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"event_time", "livesleft_sum"},
			[]interface{}{thirdWindow, 14},
			WithEventTimeField("event_time"),
		), rec))
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}

	// Adding record from window that should be cleared by GC
	{
		tx := prefixedStateStorage.BeginTransaction()
		err := processFunc.AddRecord(ctx, tx, 0, record11)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}
	// Trigger keys. No keys to trigger
	{
		tx := prefixedStateStorage.BeginTransaction()
		keys, err := processFunc.TriggerKeys(ctx, tx, 3)
		assert.Equal(t, 0, keys)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}

	time.Sleep(2 * time.Second) // Now the garbage collector should delete additional record from first window

	// Next when no record available
	{
		tx := prefixedStateStorage.BeginTransaction()
		rec, err := processFunc.Next(ctx, tx)
		assert.Nil(t, rec)
		assert.Error(t, GetErrWaitForChanges(err))
		assert.NoError(t, tx.Commit())
	}

	// Add records. (last 2 from second window, last 2 from third window)
	{
		tx := prefixedStateStorage.BeginTransaction()

		err := processFunc.AddRecord(ctx, tx, 0, record23)
		assert.NoError(t, err)
		err = processFunc.AddRecord(ctx, tx, 0, record24)
		assert.NoError(t, err)

		err = processFunc.AddRecord(ctx, tx, 0, record33)
		assert.NoError(t, err)
		err = processFunc.AddRecord(ctx, tx, 0, record34)
		assert.NoError(t, err)

		assert.NoError(t, tx.Commit())
	}
	// Set watermark to trigger records
	{
		tx := prefixedStateStorage.BeginTransaction()
		err := processFunc.UpdateWatermark(ctx, tx, now.Add(12*time.Minute)) // GC: Second window deleted (12 - 6 = 6 > secondWindow)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}
	// Trigger keys.
	{
		tx := prefixedStateStorage.BeginTransaction()
		keys, err := processFunc.TriggerKeys(ctx, tx, 3)
		assert.Equal(t, 1, keys) // ANOTHER event time deleted !!
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}
	// Trigger keys. No keys to trigger
	{
		tx := prefixedStateStorage.BeginTransaction()
		keys, err := processFunc.TriggerKeys(ctx, tx, 3)
		assert.Equal(t, 0, keys)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}

	time.Sleep(2 * time.Second) // Now the garbage collector should delete records from second window

	// Get record.
	{
		tx := prefixedStateStorage.BeginTransaction()
		rec, err := processFunc.Next(ctx, tx)
		assert.NoError(t, EqualityOfEverythingButIDs(NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"event_time", "livesleft_sum"},
			[]interface{}{thirdWindow, 14},
			WithEventTimeField("event_time"),
			WithUndo(),
		), rec))
		rec, err = processFunc.Next(ctx, tx)
		assert.NoError(t, EqualityOfEverythingButIDs(NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"event_time", "livesleft_sum"},
			[]interface{}{thirdWindow, 17},
			WithEventTimeField("event_time"),
		), rec))
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit())
	}

	// Stop garbage collector
	processFunc.garbageCollectorCtxCancel()
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
