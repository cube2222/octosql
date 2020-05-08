package execution

import (
	"context"
	"log"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql/storage"
)

type InMemoryStream struct {
	streamID *StreamID
}

func NewInMemoryStream(ctx context.Context, data []*Record) *InMemoryStream {
	streamID := GetRawStreamID()

	outputQueue := storage.NewDeque(storage.GetStateTransactionFromContext(ctx).WithPrefix(streamID.AsPrefix()).WithPrefix(outputQueuePrefix))

	for i := range data {
		err := outputQueue.PushBack(data[i])
		if err != nil {
			log.Fatal("couldn't push record to output queue in inMemory stream: ", err)
		}
	}

	return &InMemoryStream{
		streamID: streamID,
	}
}

func (ims *InMemoryStream) Close(ctx context.Context, storage storage.Storage) error {
	if err := storage.DropAll(ims.streamID.AsPrefix()); err != nil {
		return errors.Wrap(err, "couldn't clear storage with streamID prefix")
	}

	return nil
}

func (ims *InMemoryStream) Next(ctx context.Context) (*Record, error) {
	outputQueue := storage.NewDeque(storage.GetStateTransactionFromContext(ctx).WithPrefix(ims.streamID.AsPrefix()).WithPrefix(outputQueuePrefix))

	var outRecord Record
	err := outputQueue.PopFront(&outRecord)
	if err == storage.ErrNotFound {
		return nil, ErrEndOfStream
	} else if err != nil {
		return nil, errors.Wrap(err, "couldn't pop record from output queue")
	}

	return &outRecord, nil
}
