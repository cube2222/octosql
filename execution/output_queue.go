package execution

import (
	"context"
	"log"
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
	newInternals := &outputQueueInternals{
		queue:         make(chan proto.Message, 1024),
		firstElements: make(chan proto.Message, 1024),
	}

	internals, _ := outputQueues.LoadOrStore(id, newInternals)

	return &OutputQueue{
		internal: internals.(*outputQueueInternals),
		tx:       tx,
	}
}

var queueElementsPrefix = []byte("$queue_elements$")

func (q *OutputQueue) Push(ctx context.Context, element proto.Message) error {
	log.Printf("*** Push %s %s", q.tx.Prefix(), element)
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
		log.Printf("*** Peek %s %s", q.tx.Prefix(), msg.String())
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
		log.Printf("*** Peek %s %s", q.tx.Prefix(), msg.String())
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
		log.Printf("*** Pop %s %s", q.tx.Prefix(), msg.String())
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
		log.Printf("*** Pop %s %s", q.tx.Prefix(), msg.String())
		return nil
	default:
	}

	log.Printf("*** Pop sending subscription %s", q.tx.Prefix())

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
