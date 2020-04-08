package execution

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

type nodeToGet struct {
	shuffleID *ShuffleID
	node      Node
	variables octosql.Variables
}

func GetAndStartAllShuffles(ctx context.Context, stateStorage storage.Storage, tx storage.StateTransaction, nodes []Node, variables octosql.Variables) ([]RecordStream, []*ExecutionOutput, error) {
	nextShuffleIDRaw, err := GetSourceStreamID(tx.WithPrefix([]byte("$root")), octosql.MakeString("next_shuffle_id"))
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get next shuffle id")
	}
	nextShuffleID := (*ShuffleID)(nextShuffleIDRaw)

	nextShuffles := make(map[string]ShuffleData)

	outRecordStreams := make([]RecordStream, len(nodes))
	outExecOutputs := make([]*ExecutionOutput, len(nodes))
	for partition, node := range nodes {
		sourceStreamID, err := GetSourceStreamID(tx.WithPrefix(nextShuffleID.AsPrefix()), octosql.MakeString(fmt.Sprintf("shuffle_input_%d", partition)))
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't get new source stream ID for shuffle with ID %s", nextShuffleID.Id)
		}

		newPipelineMetadata := PipelineMetadata{
			NextShuffleID: nextShuffleID,
			Partition:     partition,
		}

		rs, execOutput, err := node.Get(context.WithValue(ctx, pipelineMetadataContextKey{}, newPipelineMetadata), variables, sourceStreamID)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't get new source stream for shuffle with ID %s", nextShuffleID.Id)
		}

		for id, data := range execOutput.NextShuffles {
			nextShuffles[id] = data
		}

		outRecordStreams[partition] = rs
		outExecOutputs[partition] = execOutput
	}

	for len(nextShuffles) > 0 {
		newNextShuffles := make(map[string]ShuffleData)

		for _, shuffle := range nextShuffles {
			tmpNextShuffles, err := shuffle.Shuffle.StartSources(ctx, stateStorage, tx, shuffle.ShuffleID, shuffle.Variables)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "couldn't start sources for shuffle with id %v", err)
			}

			for id, data := range tmpNextShuffles {
				newNextShuffles[id] = data
			}
		}

		nextShuffles = newNextShuffles
	}

	return outRecordStreams, outExecOutputs, nil
}

// TODO: The main queue namespaced by shuffle ID, not streamID.

type pipelineMetadataContextKey struct{}

type PipelineMetadata struct {
	NextShuffleID *ShuffleID
	Partition     int
}

type ShuffleID StreamID

func (id *ShuffleID) AsPrefix() []byte {
	return (*StreamID)(id).AsPrefix()
}

func (id *ShuffleID) MapKey() string {
	return id.Id
}

type Shuffle struct {
	shuffleSources []Node
}

func (s *Shuffle) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, *ExecutionOutput, error) {
	pipelineMetadata := ctx.Value(pipelineMetadataContextKey{}).(*PipelineMetadata)

	receiver := NewShuffleReceiver(streamID, pipelineMetadata.NextShuffleID, pipelineMetadata.Partition, len(s.shuffleSources))
	execOutput := NewExecutionOutput(receiver)
	execOutput.NextShuffles = map[string]ShuffleData{
		pipelineMetadata.NextShuffleID.MapKey(): {
			Shuffle:   s,
			Variables: variables,
			ShuffleID: pipelineMetadata.NextShuffleID,
		},
	}

	return receiver, execOutput, nil

	/*phantom := octosql.MakePhantom()
	if err := inputsAlreadyStartedState.Get(&phantom); err == storage.ErrNotFound {
		if err := inputsAlreadyStartedState.Set(&phantom); err != nil {
			return nil, nil, errors.Wrap(err, "couldn't save that inputs have already been started")
		}

		nextShuffleIDRaw, err := GetSourceStreamID(shuffleScopedTx, octosql.MakeString("next_shuffle"))
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't get next shuffle id")
		}
		nextShuffleID := (*ShuffleID)(nextShuffleIDRaw)

		for partition := 0; partition < len(s.shuffleSources); partition++ {
			sourceStreamID, err := GetSourceStreamID(shuffleScopedTx, octosql.MakeString(fmt.Sprintf("shuffle_input_%d", partition)))
			if err != nil {
				return nil, nil, errors.Wrapf(err, "couldn't get new source stream ID for stream with ID %s", streamID.String())
			}

			newPipelineMetadata := PipelineMetadata{
				NextShuffleID: nextShuffleID,
				Partition:     partition,
			}

			rs, execOutput, err := s.shuffleSources[partition].Get(context.WithValue(ctx, pipelineMetadataContextKey{}, newPipelineMetadata), variables, sourceStreamID)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "couldn't get new source stream for stream with ID %s", streamID.String())
			}

			execOutput.Pipelines
		}
	} else {
		return nil, nil, errors.Wrap(err, "couldn't get if inputs have already been started")
	}*/
}

func (s *Shuffle) StartSources(ctx context.Context, stateStorage storage.Storage, tx storage.StateTransaction, shuffleID *ShuffleID, variables octosql.Variables) (map[string]ShuffleData, error) {
	nextShuffleIDRaw, err := GetSourceStreamID(tx.WithPrefix(shuffleID.AsPrefix()), octosql.MakeString("next_shuffle_id"))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get next shuffle id")
	}
	nextShuffleID := (*ShuffleID)(nextShuffleIDRaw)

	nextShuffles := make(map[string]ShuffleData)

	for partition, node := range s.shuffleSources {
		sourceStreamID, err := GetSourceStreamID(tx.WithPrefix(shuffleID.AsPrefix()), octosql.MakeString(fmt.Sprintf("shuffle_input_%d", partition)))
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't get new source stream ID for shuffle with ID %s", nextShuffleID.Id)
		}

		newPipelineMetadata := PipelineMetadata{
			NextShuffleID: nextShuffleID,
			Partition:     partition,
		}

		rs, execOutput, err := node.Get(context.WithValue(ctx, pipelineMetadataContextKey{}, newPipelineMetadata), variables, sourceStreamID)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't get new source stream for shuffle with ID %s", nextShuffleID.Id)
		}

		for id, data := range execOutput.NextShuffles {
			nextShuffles[id] = data
		}

		senderStreamID, err := GetSourceStreamID(tx.WithPrefix(nextShuffleID.AsPrefix()), octosql.MakeString(fmt.Sprintf("shuffle_sender_%d", partition)))
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't get new source stream ID for shuffle with ID %s", nextShuffleID.Id)
		}

		// TODO: Empty streamID in pull engine will still result in $$ as a prefix. We have to get rid of that.
		sender := NewShuffleSender(senderStreamID, shuffleID, partition)

		panic("fix stream ID in pull engine")
		engine := NewPullEngine(sender, stateStorage, rs, nil, execOutput.WatermarkSource) // TODO: Fix streamID

		go engine.Run(ctx)
	}

	return nextShuffles, nil
}

type ShuffleReceiver struct {
	streamID             *StreamID
	shuffleID            *ShuffleID
	partition            int
	sourcePartitionCount int
}

func NewShuffleReceiver(streamID *StreamID, shuffleID *ShuffleID, partition int, sourcePartitionCount int) *ShuffleReceiver {
	return &ShuffleReceiver{
		streamID:             streamID,
		shuffleID:            shuffleID,
		partition:            partition,
		sourcePartitionCount: sourcePartitionCount,
	}
}

var watermarksPrefix = []byte("$watermarks$")

func (node *ShuffleReceiver) Next(ctx context.Context) (*Record, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	streamPrefixedTx := tx.WithPrefix(node.streamID.AsPrefix())
	endOfStreamsMap := storage.NewMap(streamPrefixedTx.WithPrefix(endsOfStreamsPrefix))
	watermarksMap := storage.NewMap(streamPrefixedTx.WithPrefix(watermarksPrefix))

	// We want to randomize the order so we don't always read from the first input if records are available.
	sourceOrder := rand.Perm(node.sourcePartitionCount)

	changeSubscriptions := make([]*storage.Subscription, node.sourcePartitionCount)

sourcePartitionsLoop:
	for orderIndex, sourcePartition := range sourceOrder {
		// Here we try to get a record from the sourcePartition'th source partition.

		// First check if this stream hasn't been closed already.
		sourcePartitionValue := octosql.MakeInt(sourcePartition)
		var endOfStream octosql.Value
		err := endOfStreamsMap.Get(&sourcePartitionValue, &endOfStream)
		if err == storage.ErrNotFound {
		} else if err != nil {
			return nil, errors.Wrapf(err, "couldn't get end of stream for source partition with index %d", sourcePartition)
		} else if err == nil {
			// If found it means it's true which means there's nothing to read on this stream.
			continue
		}

		var sourcePartitionOutputQueue = NewOutputQueue(
			tx.
				WithPrefix([]byte(fmt.Sprintf("$%d$", node.partition))).
				WithPrefix([]byte(fmt.Sprintf("$%d$", sourcePartition))),
		)

	queuePoppingLoop:
		for {

			var element QueueElement
			err = sourcePartitionOutputQueue.Pop(ctx, &element)
			if errors.Cause(err) == ErrNewTransactionRequired {
				return nil, err
			} else if errWaitForChanges := GetErrWaitForChanges(err); errWaitForChanges != nil {
				// We save this subscription, as we'll later wait on all the streams at once
				// if others will respond with this error too.
				changeSubscriptions[sourcePartition] = errWaitForChanges.Subscription
				continue sourcePartitionsLoop
			} else if err != nil {
				return nil, errors.Wrapf(err, "couldn't get next record from source partition with index %d", sourcePartition)
			}

			switch el := element.Type.(type) {
			case *QueueElement_Record:
				// We got a record, so we close all the received subscriptions from the previous streams.
				for _, sourceIndexToClose := range sourceOrder[:orderIndex] {
					if changeSubscriptions[sourceIndexToClose] == nil {
						continue
					}
					err := changeSubscriptions[sourceIndexToClose].Close()
					if err != nil {
						return nil, errors.Wrapf(err, "couldn't close changes subscription for source partition with index %d", sourceIndexToClose)
					}
				}

				return el.Record, nil

			case *QueueElement_Watermark:
				if err := watermarksMap.Set(&sourcePartitionValue, el.Watermark); err != nil {
					return nil, errors.Wrapf(err, "couldn't set new watermark value for source partition with index %d", sourcePartition)
				}
				continue queuePoppingLoop

			case *QueueElement_EndOfStream:
				phantom := octosql.MakePhantom()
				if err := endOfStreamsMap.Set(&sourcePartitionValue, &phantom); err != nil {
					return nil, errors.Wrapf(err, "couldn't set end of stream for source partition with index %d", sourcePartition)
				}
				continue sourcePartitionsLoop

			case *QueueElement_Error:
				return nil, errors.Wrapf(errors.New(el.Error), "received error from source partition with index %d", sourcePartition)

			default:
				panic("unreachable")
			}
		}
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

func (node *ShuffleReceiver) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	streamPrefixedTx := tx.WithPrefix(node.streamID.AsPrefix())
	watermarksMap := storage.NewMap(streamPrefixedTx.WithPrefix(watermarksPrefix))

	var earliestWatermark time.Time

	for sourcePartition := 0; sourcePartition < node.sourcePartitionCount; sourcePartition++ {
		sourcePartitionValue := octosql.MakeInt(sourcePartition)

		var protoTimestamp timestamp.Timestamp
		err := watermarksMap.Get(&sourcePartitionValue, &protoTimestamp)
		if err == storage.ErrNotFound {
			return time.Time{}, nil
		} else if err != nil {
			return time.Time{}, errors.Wrapf(err, "couldn't get watermark value for source partition %d", sourcePartition)
		}

		curWatermark, err := ptypes.Timestamp(&protoTimestamp)
		if err != nil {
			return time.Time{}, errors.Wrapf(err, "couldn't parse proto timestamp as time for source partition %d", sourcePartition)
		}

		if sourcePartition == 0 { // earliestWatermark is not set yet
			earliestWatermark = curWatermark
		} else if curWatermark.Before(earliestWatermark) {
			earliestWatermark = curWatermark
		}
	}

	return earliestWatermark, nil
}

func (node *ShuffleReceiver) Close() error {
	// TODO: Cleanup
	return nil
}

type ShuffleSender struct {
	streamID  *StreamID
	shuffleID *ShuffleID
	partition int
}

func NewShuffleSender(streamID *StreamID, shuffleID *ShuffleID, partition int) *ShuffleSender {
	return &ShuffleSender{
		streamID:  streamID,
		shuffleID: shuffleID,
		partition: partition,
	}
}

func (s *ShuffleSender) ReadyForMore(ctx context.Context, tx storage.StateTransaction) error {
	panic("implement me")
}

func (s *ShuffleSender) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, record *Record) error {
	panic("implement me")
}

func (s *ShuffleSender) Next(ctx context.Context, tx storage.StateTransaction) (*Record, error) {
	panic("implement me")
}

func (s *ShuffleSender) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	panic("implement me")
}

func (s *ShuffleSender) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	panic("implement me")
}

func (s *ShuffleSender) MarkEndOfStream(ctx context.Context, tx storage.StateTransaction) error {
	panic("implement me")
}

func (s *ShuffleSender) MarkError(ctx context.Context, tx storage.StateTransaction, err error) error {
	panic("implement me")
}

func (s *ShuffleSender) Close() error {
	panic("implement me")
}
