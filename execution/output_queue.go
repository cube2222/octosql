package execution

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql/streaming/storage"
)

type OutputQueue struct {
	tx storage.StateTransaction
}

func NewOutputQueue(tx storage.StateTransaction) *OutputQueue {
	return &OutputQueue{
		tx: tx,
	}
}

var queueElementsPrefix = []byte("$queue_elements$")

func (q *OutputQueue) Push(ctx context.Context, element proto.Message) error {
	queueElements := storage.NewDeque(q.tx.WithPrefix(queueElementsPrefix))

	err := queueElements.PushBack(element)
	if err != nil {
		return errors.Wrap(err, "couldn't append element to queue")
	}
	return nil
}

func (q *OutputQueue) Peek(ctx context.Context, msg proto.Message) error {
	queueElements := storage.NewDeque(q.tx.WithPrefix(queueElementsPrefix))

	err := queueElements.PeekFront(msg)
	if err == storage.ErrNotFound {
		return storage.ErrNotFound
	} else if err != nil {
		return errors.Wrap(err, "couldn't pop element from queue")
	}

	return nil
}

func (q *OutputQueue) Pop(ctx context.Context, msg proto.Message) error {
	queueElements := storage.NewDeque(q.tx.WithPrefix(queueElementsPrefix))

	err := queueElements.PopFront(msg)
	if err == storage.ErrNotFound {
		subscription := q.tx.GetUnderlyingStorage().Subscribe(ctx)

		curTx := q.tx.GetUnderlyingStorage().BeginTransaction()
		defer curTx.Abort()
		curQueueElements := storage.NewDeque(curTx.WithPrefix(queueElementsPrefix))

		err := curQueueElements.PeekFront(msg)
		if err == storage.ErrNotFound {
			return NewErrWaitForChanges(subscription)
		} else {
			if subErr := subscription.Close(); subErr != nil {
				return errors.Wrap(subErr, "couldn't close subscription")
			}
			if err == nil {
				return ErrNewTransactionRequired
			} else {
				return errors.Wrap(err, "couldn't check if there are elements in the queue out of transaction")
			}
		}
	} else if err != nil {
		return errors.Wrap(err, "couldn't pop element from queue")
	}

	return nil
}
