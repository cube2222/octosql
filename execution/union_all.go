package execution

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"

	"github.com/pkg/errors"
)

type UnionAll struct {
	sources []Node
}

func NewUnionAll(sources ...Node) *UnionAll {
	return &UnionAll{sources: sources}
}

func (node *UnionAll) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, error) {
	prefixedTx := storage.GetStateTransactionFromContext(ctx).WithPrefix(streamID.AsPrefix())

	sourceRecordStreams := make([]RecordStream, len(node.sources))
	for i := range node.sources {
		sourceStreamID, err := GetSourceStreamID(prefixedTx, octosql.MakeInt(i))
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't get source stream ID for source with index %d", i)
		}
		recordStream, err := node.sources[i].Get(ctx, variables, sourceStreamID)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't get source record stream with index %d", i)
		}
		sourceRecordStreams[i] = recordStream
	}

	return &UnifiedStream{
		sources:  sourceRecordStreams,
		streamID: streamID,
	}, nil
}

type UnifiedStream struct {
	sources  []RecordStream
	streamID *StreamID
}

func (node *UnifiedStream) Close() error {
	for i := range node.sources {
		err := node.sources[i].Close()
		if err != nil {
			return errors.Wrapf(err, "couldn't close source stream with index %d", i)
		}
	}

	return nil
}

var endsOfStreamsPrefix = []byte("$end_of_streams$")

func (node *UnifiedStream) Next(ctx context.Context) (*Record, error) {
	tx := storage.GetStateTransactionFromContext(ctx).WithPrefix(node.streamID.AsPrefix())
	endOfStreamsMap := storage.NewMap(tx.WithPrefix(endsOfStreamsPrefix))

	changeSubscriptions := make([]*storage.Subscription, len(node.sources))
	for i := range node.sources { // TODO: Think about randomizing order.
		key := octosql.MakeString(fmt.Sprint(i))
		var endOfStream octosql.Value
		err := endOfStreamsMap.Get(&key, &endOfStream)
		if err == storage.ErrNotFound {
		} else if err != nil {
			return nil, errors.Wrapf(err, "couldn't get end of stream for source stream with index %d", i)
		} else if err == nil {
			// If found it means it's true which means there's nothing to read on this stream.
			continue
		}

		record, err := node.sources[i].Next(ctx)
		if err == ErrEndOfStream {
			endOfStream = octosql.MakeBool(true)
			err := endOfStreamsMap.Set(&key, &endOfStream)
			if err != nil {
				return nil, errors.Wrapf(err, "couldn't set end of stream for source stream with index %d", i)
			}
			continue
		} else if errors.Cause(err) == ErrNewTransactionRequired {
			return nil, err
		} else if errWaitForChanges := GetErrWaitForChanges(err); errWaitForChanges != nil {
			changeSubscriptions[i] = errWaitForChanges.Subscription
			continue
		} else if err != nil {
			return nil, errors.Wrapf(err, "couldn't get next record from source stream with index %d", i)
		}

		for j := 0; j < i; j++ {
			if changeSubscriptions[j] == nil {
				continue
			}
			err := changeSubscriptions[j].Close()
			if err != nil {
				return nil, errors.Wrapf(err, "couldn't close changes subscription for source stream with index %d", j)
			}
		}

		return record, nil
	}

	changeSubscriptionsNonNil := make([]*storage.Subscription, 0)
	for i := range changeSubscriptions {
		if changeSubscriptions[i] != nil {
			changeSubscriptionsNonNil = append(changeSubscriptionsNonNil, changeSubscriptions[i])
		}
	}

	if len(changeSubscriptionsNonNil) == 0 {
		return nil, ErrEndOfStream
	}

	return nil, NewErrWaitForChanges(storage.ConcatSubscriptions(ctx, changeSubscriptionsNonNil...))
}
