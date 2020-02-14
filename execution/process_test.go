package execution

import (
	"context"
	"log"
	"testing"

	"github.com/dgraph-io/badger/v2"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/stretchr/testify/assert"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
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
	element, err := queue.Pop(ctx)
	assert.Nil(t, err)
	return element
}

func TestOutputQueue_Ok(t *testing.T) {
	opts := badger.DefaultOptions("test")
	opts.InMemory = true
	opts.Dir = ""
	opts.ValueDir = ""
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	stateStorage := storage.NewBadgerStorage(db)
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
	opts := badger.DefaultOptions("test")
	opts.InMemory = true
	opts.Dir = ""
	opts.ValueDir = ""
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	stateStorage := storage.NewBadgerStorage(db)
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
	opts := badger.DefaultOptions("test")
	opts.InMemory = true
	opts.Dir = ""
	opts.ValueDir = ""
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	stateStorage := storage.NewBadgerStorage(db)
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

		_, err := readQueue.Pop(ctx)
		assert.Equal(t, err, ErrNewTransactionRequired)
		readTx.Abort()

		readTx = stateStorage.BeginTransaction()
		readQueue = NewOutputQueue(readTx)
		assert.True(t, proto.Equal(recordElement, GetElementAssertNoError(t, ctx, readQueue)))
		assert.Nil(t, readTx.Commit())
	}
}

/*func TestOutputQueue_WaitForChanges(t *testing.T) {
	opts := badger.DefaultOptions("test")
	opts.InMemory = true
	opts.Dir = ""
	opts.ValueDir = ""
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	stateStorage := storage.NewBadgerStorage(db)
	ctx := context.Background()

	{
		readTx := stateStorage.BeginTransaction()
		readQueue := NewOutputQueue(readTx)
		_, err := readQueue.Pop(ctx)
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
}*/
