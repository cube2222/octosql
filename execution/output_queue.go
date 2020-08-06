package execution

import (
	"context"
	"sync"

	"github.com/golang/protobuf/proto"

	"github.com/cube2222/octosql/storage"
)

var outputQueues = sync.Map{}
var outputQueuesFirstElement = sync.Map{}

type OutputQueue struct {
	tx            storage.StateTransaction
	channel       chan proto.Message
	firstElements chan proto.Message
}

func NewOutputQueue(tx storage.StateTransaction) *OutputQueue {
	id := tx.WithPrefix(queueElementsPrefix).Prefix()
	newQueueChan := make(chan proto.Message, 1024)
	newFirstElementsChan := make(chan proto.Message, 1024)

	channel, _ := outputQueues.LoadOrStore(id, newQueueChan)
	firstElements, _ := outputQueues.LoadOrStore(id, newFirstElementsChan)

	return &OutputQueue{
		channel:       channel.(chan proto.Message),
		firstElements: firstElements.(chan proto.Message),
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
