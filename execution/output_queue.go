package execution

import (
	"context"
	"sync"

	"github.com/golang/protobuf/proto"

	"github.com/cube2222/octosql/storage"
)

// TODO: All of this doesn't preserve ordering. Which is baaaaad. Because a record can be put behind an end of stream message.

var outputQueues = sync.Map{}
var outputQueuesFirstElement = sync.Map{}
var outputQueuesLockers = sync.Map{}

type outputQueueInternals struct {
	queue         chan proto.Message
	firstElements chan proto.Message
}

type OutputQueue struct {
	tx       storage.StateTransaction
	internal *outputQueueInternals
}

func NewOutputQueue(tx storage.StateTransaction) *OutputQueue {
	id := tx.WithPrefix(queueElementsPrefix).Prefix()

	actualInternals, ok := outputQueues.Load(id)
	if !ok {
		newInternals := &outputQueueInternals{
			queue:         make(chan proto.Message, 1024),
			firstElements: make(chan proto.Message, 1024),
		}

		actualInternals, _ = outputQueues.LoadOrStore(id, newInternals)
	}

	return &OutputQueue{
		internal: actualInternals.(*outputQueueInternals),
		tx:       tx,
	}
}

var queueElementsPrefix = []byte("$queue_elements$")

func (q *OutputQueue) Push(ctx context.Context, element proto.Message) error {
	q.internal.queue <- element

	return nil
}

func (q *OutputQueue) Peek(ctx context.Context, msg proto.Message) error {
	select {
	case elem := <-q.internal.firstElements:
		q.internal.firstElements <- elem
		data, err := proto.Marshal(elem)
		if err != nil {
			return err
		}
		if err := proto.Unmarshal(data, msg); err != nil {
			return err
		}
		return nil
	default:
	}

	select {
	case elem := <-q.internal.queue:
		q.internal.firstElements <- elem
		data, err := proto.Marshal(elem)
		if err != nil {
			return err
		}
		if err := proto.Unmarshal(data, msg); err != nil {
			return err
		}
	default:
		return storage.ErrNotFound
	}

	return nil
}

func (q *OutputQueue) Pop(ctx context.Context, msg proto.Message) error {
	select {
	case elem := <-q.internal.firstElements:
		data, err := proto.Marshal(elem)
		if err != nil {
			return err
		}
		if err := proto.Unmarshal(data, msg); err != nil {
			return err
		}
		return nil
	default:
	}

	select {
	case elem := <-q.internal.queue:
		data, err := proto.Marshal(elem)
		if err != nil {
			return err
		}
		if err := proto.Unmarshal(data, msg); err != nil {
			return err
		}
		return nil
	default:
	}

	subscription := storage.NewSubscription(ctx, func(ctx context.Context, changes chan<- struct{}) error {
		select {
		case elem := <-q.internal.firstElements:
			q.internal.firstElements <- elem

			select {
			case changes <- struct{}{}:
				return storage.ErrChangeSent
			case <-ctx.Done():
				return ctx.Err()
			}

		case elem := <-q.internal.queue:
			q.internal.firstElements <- elem

			select {
			case changes <- struct{}{}:
				return storage.ErrChangeSent
			case <-ctx.Done():
				return ctx.Err()
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	})
	return NewErrWaitForChanges(subscription)
}
