package execution

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/stretchr/testify/assert"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

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
