package execution

import (
	"context"
	"time"

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

func (node *UnionAll) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, *ExecutionOutput, error) {
	prefixedTx := storage.GetStateTransactionFromContext(ctx).WithPrefix(streamID.AsPrefix())

	sourceRecordStreams := make([]RecordStream, len(node.sources))
	sourceWatermarkSources := make([]WatermarkSource, len(node.sources))
	for sourceIndex := range node.sources {
		sourceStreamID, err := GetSourceStreamID(prefixedTx, octosql.MakeInt(sourceIndex))
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't get source stream ID for source with index %d", sourceIndex)
		}

		recordStream, execOutput, err := node.sources[sourceIndex].Get(ctx, variables, sourceStreamID)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't get source record stream with index %d", sourceIndex)
		}

		sourceRecordStreams[sourceIndex] = recordStream
		sourceWatermarkSources[sourceIndex] = execOutput.WatermarkSource
	}

	us := &UnifiedStream{
		sources:          sourceRecordStreams,
		watermarkSources: sourceWatermarkSources,
		streamID:         streamID,
	}

	return us, NewExecutionOutput(us), nil
}

type UnifiedStream struct {
	sources          []RecordStream
	watermarkSources []WatermarkSource
	streamID         *StreamID
}

func (node *UnifiedStream) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	var earliestWatermark time.Time

	for sourceIndex := range node.sources {
		sourceWatermark, err := node.watermarkSources[sourceIndex].GetWatermark(ctx, tx)
		if err != nil {
			return time.Time{}, errors.Wrapf(err, "couldn't get current watermark from source %d", sourceIndex)
		}

		if sourceIndex == 0 { // earliestWatermark is not set yet
			earliestWatermark = sourceWatermark
		} else if sourceWatermark.Before(earliestWatermark) {
			earliestWatermark = sourceWatermark
		}
	}

	return earliestWatermark, nil
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
	for sourceIndex := range node.sources { // TODO: Think about randomizing order.
		// Here we try to get a record from the sourceIndex'th source stream.

		// First check if this stream hasn't been closed already.
		indexValue := octosql.MakeInt(sourceIndex)
		var endOfStream octosql.Value
		err := endOfStreamsMap.Get(&indexValue, &endOfStream)
		if err == storage.ErrNotFound {
		} else if err != nil {
			return nil, errors.Wrapf(err, "couldn't get end of stream for source stream with index %d", sourceIndex)
		} else if err == nil {
			// If found it means it's true which means there's nothing to read on this stream.
			continue
		}

		record, err := node.sources[sourceIndex].Next(ctx)
		if err == ErrEndOfStream {
			// We save that this stream is over
			endOfStream = octosql.MakeBool(true)
			err := endOfStreamsMap.Set(&indexValue, &endOfStream)
			if err != nil {
				return nil, errors.Wrapf(err, "couldn't set end of stream for source stream with index %d", sourceIndex)
			}
			continue
		} else if errors.Cause(err) == ErrNewTransactionRequired {
			return nil, err
		} else if errWaitForChanges := GetErrWaitForChanges(err); errWaitForChanges != nil {
			// We save this subscription, as we'll later wait on all the streams at once
			// if others will respond with this error too.
			changeSubscriptions[sourceIndex] = errWaitForChanges.Subscription
			continue
		} else if err != nil {
			return nil, errors.Wrapf(err, "couldn't get next record from source stream with index %d", sourceIndex)
		}

		// We got a record, so we close all the received subscriptions from the previous streams.
		for i := 0; i < sourceIndex; i++ {
			if changeSubscriptions[i] == nil {
				continue
			}
			err := changeSubscriptions[i].Close()
			if err != nil {
				return nil, errors.Wrapf(err, "couldn't close changes subscription for source stream with index %d", i)
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
