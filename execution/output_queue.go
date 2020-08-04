package execution

import (
	"context"
	"sync"

	"github.com/golang/protobuf/proto"

	"github.com/cube2222/octosql/storage"
)

var outputQueuesMutex = sync.RWMutex{}
var outputQueues = make(map[string]chan proto.Message)
var outputQueuesFirstElement = make(map[string]chan proto.Message)

type OutputQueue struct {
	tx            storage.StateTransaction
	channel       chan proto.Message
	firstElements chan proto.Message
}

func NewOutputQueue(tx storage.StateTransaction) *OutputQueue {
	outputQueuesMutex.Lock()
	id := tx.WithPrefix(queueElementsPrefix).Prefix()
	channel, ok := outputQueues[id]
	if !ok {
		channel = make(chan proto.Message, 1024)
		outputQueues[id] = channel
		outputQueuesFirstElement[id] = make(chan proto.Message, 1024)
	}
	firstElements := outputQueuesFirstElement[id]
	outputQueuesMutex.Unlock()

	return &OutputQueue{
		channel:       channel,
		firstElements: firstElements,
		tx:            tx,
	}
}

var queueElementsPrefix = []byte("$queue_elements$")

func (q *OutputQueue) Push(ctx context.Context, element proto.Message) error {
	q.channel <- element

	return nil
}

func (q *OutputQueue) Peek(ctx context.Context, msg proto.Message) error {
	select {
	case elem := <-q.firstElements:
		q.firstElements <- elem
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
	case elem := <-q.channel:
		q.firstElements <- elem
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
	case elem := <-q.firstElements:
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
	case elem := <-q.channel:
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
		case elem := <-q.firstElements:
			q.firstElements <- elem

			select {
			case changes <- struct{}{}:
				return storage.ErrChangeSent
			case <-ctx.Done():
				return ctx.Err()
			}

		case elem := <-q.channel:
			q.firstElements <- elem

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
