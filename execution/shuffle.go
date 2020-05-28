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
	"github.com/cube2222/octosql/storage"
)

// This is used to start the whole plan. It starts each phase (separated by shuffles) one by one
// and takes care to properly pass shuffle ID's to shuffle receivers and senders.
func GetAndStartAllShuffles(ctx context.Context, stateStorage storage.Storage, rootStreamID *StreamID, nodes []Node, variables octosql.Variables) ([]RecordStream, []*ExecutionOutput, error) {
	tx := stateStorage.BeginTransaction()

	ctx = storage.InjectStateTransaction(ctx, tx)
	nextShuffleID, err := GetSourceShuffleID(tx.WithPrefix(rootStreamID.AsPrefix()), octosql.MakeString("next_shuffle_id"))
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get next shuffle id")
	}

	if err := tx.Commit(); err != nil {
		return nil, nil, errors.Wrap(err, "couldn't commit transaction to get initial next shuffle id")
	}

	nextShuffles := make(map[string]ShuffleData)

	// We start the head node to possibly get the first shuffle
	outRecordStreams := make([]RecordStream, len(nodes))
	outExecOutputs := make([]*ExecutionOutput, len(nodes))
	for partition, node := range nodes {
		tx := stateStorage.BeginTransaction()
		ctx := storage.InjectStateTransaction(ctx, tx)

		sourceStreamID, err := GetSourceStreamID(tx.WithPrefix(rootStreamID.AsPrefix()), octosql.MakeString(fmt.Sprintf("shuffle_input_%d", partition)))
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't get new source stream ID for root stream ID %s", rootStreamID.Id)
		}

		// These are parameters for the _next_ shuffle.
		newPipelineMetadata := PipelineMetadata{
			NextShuffleID: nextShuffleID,
			Partition:     partition,
		}

		rs, execOutput, err := node.Get(context.WithValue(ctx, pipelineMetadataContextKey{}, newPipelineMetadata), variables, sourceStreamID)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't get new source stream for root stream with ID %s", rootStreamID.Id)
		}

		if err := tx.Commit(); err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't commit transaction to get first shuffle output with partition %d", partition)
		}

		for _, task := range execOutput.TasksToRun {
			go task() // TODO: Error handling, or maybe gather in main?
		}

		for id, data := range execOutput.NextShuffles {
			nextShuffles[id] = data
		}

		outRecordStreams[partition] = rs
		outExecOutputs[partition] = execOutput
	}

	// Now iteratively start all shuffles.
	for len(nextShuffles) > 0 {
		newNextShuffles := make(map[string]ShuffleData)

		for _, shuffle := range nextShuffles {
			tmpNextShuffles, err := shuffle.Shuffle.StartSources(ctx, stateStorage, shuffle.ShuffleID, shuffle.Variables)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "couldn't start sources for shuffle with id %v", shuffle.ShuffleID.Id)
			}

			// Deduplicate shuffles by their ID's
			for id, data := range tmpNextShuffles {
				newNextShuffles[id] = data
			}
		}

		nextShuffles = newNextShuffles
	}

	return outRecordStreams, outExecOutputs, nil
}

type pipelineMetadataContextKey struct{}

type PipelineMetadata struct {
	// The ID for the next shuffle.
	NextShuffleID *ShuffleID

	// The partition of the current stream.
	Partition int
}

func NewShuffleID(id string) *ShuffleID {
	return &ShuffleID{
		Id: id,
	}
}

func (id *ShuffleID) AsPrefix() []byte {
	return []byte("$" + id.Id + "$")
}

func (id *ShuffleID) AsMapKey() string {
	return id.Id
}

type Shuffle struct {
	outputPartitionCount int
	strategyPrototype    ShuffleStrategyPrototype
	sources              []Node
}

func NewShuffle(outputPartitionCount int, strategyPrototype ShuffleStrategyPrototype, sources []Node) *Shuffle {
	return &Shuffle{
		outputPartitionCount: outputPartitionCount,
		strategyPrototype:    strategyPrototype,
		sources:              sources,
	}
}

func (s *Shuffle) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, *ExecutionOutput, error) {
	pipelineMetadata := ctx.Value(pipelineMetadataContextKey{}).(PipelineMetadata)

	receiver := NewShuffleReceiver(streamID, pipelineMetadata.NextShuffleID, len(s.sources), pipelineMetadata.Partition)
	execOutput := NewExecutionOutput(receiver, map[string]ShuffleData{
		pipelineMetadata.NextShuffleID.AsMapKey(): {
			Shuffle:   s,
			Variables: variables,
			ShuffleID: pipelineMetadata.NextShuffleID,
		},
	}, nil)

	return receiver, execOutput, nil
}

func (s *Shuffle) StartSources(ctx context.Context, stateStorage storage.Storage, shuffleID *ShuffleID, variables octosql.Variables) (map[string]ShuffleData, error) {
	tx := stateStorage.BeginTransaction()

	// Create an ID for the possible _even next_ shuffle.
	nextShuffleID, err := GetSourceShuffleID(tx.WithPrefix(shuffleID.AsPrefix()), octosql.MakeString("next_shuffle_id"))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get next shuffle id")
	}

	if err := tx.Commit(); err != nil {
		return nil, errors.Wrap(err, "couldn't commit transaction to save next shuffle ID")
	}

	nextShuffles := make(map[string]ShuffleData)

	for partition, node := range s.sources {
		tx := stateStorage.BeginTransaction()
		ctx := storage.InjectStateTransaction(ctx, tx)

		sourceStreamID, err := GetSourceStreamID(tx.WithPrefix(shuffleID.AsPrefix()), octosql.MakeString(fmt.Sprintf("shuffle_input_%d", partition)))
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't get new source stream ID for shuffle with ID %s", shuffleID.Id)
		}

		newPipelineMetadata := PipelineMetadata{
			NextShuffleID: nextShuffleID,
			Partition:     partition,
		}

		// Get the shuffle source stream.
		rs, execOutput, err := node.Get(context.WithValue(ctx, pipelineMetadataContextKey{}, newPipelineMetadata), variables, sourceStreamID)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't get new source stream for shuffle with ID %s", shuffleID.Id)
		}

		// Add deduplicated next shuffle information.
		for id, data := range execOutput.NextShuffles {
			nextShuffles[id] = data
		}

		senderStreamID, err := GetSourceStreamID(tx.WithPrefix(shuffleID.AsPrefix()), octosql.MakeString(fmt.Sprintf("shuffle_sender_%d", partition)))
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't get new source stream ID for shuffle with ID %s", shuffleID.Id)
		}

		strategy, err := s.strategyPrototype.Get(ctx, variables)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't get strategy for shuffle with ID %s", shuffleID.Id)
		}

		if err := tx.Commit(); err != nil {
			return nil, errors.Wrapf(err, "couldn't commit transaction to run shuffle sender from partition %d", partition)
		}

		for _, task := range execOutput.TasksToRun {
			go task() // TODO: Error handling, or maybe gather in main?
		}

		// Start the shuffle sender.
		sender := NewShuffleSender(senderStreamID, shuffleID, strategy, s.outputPartitionCount, partition)

		engine := NewPullEngine(sender, stateStorage, []RecordStream{rs}, nil, execOutput.WatermarkSource, false, ctx)

		go engine.Run()
	}

	return nextShuffles, nil
}

// ShuffleReceiver is a RecordStream abstraction on a shuffle and receives records from it for a partition.
type ShuffleReceiver struct {
	streamID             *StreamID
	shuffleID            *ShuffleID
	sourcePartitionCount int
	partition            int

	received int
}

func NewShuffleReceiver(streamID *StreamID, shuffleID *ShuffleID, sourcePartitionCount int, partition int) *ShuffleReceiver {
	return &ShuffleReceiver{
		streamID:             streamID,
		shuffleID:            shuffleID,
		sourcePartitionCount: sourcePartitionCount,
		partition:            partition,
	}
}

var watermarksPrefix = []byte("$watermarks$")

func getQueuePrefix(from, to int) []byte {
	return []byte(fmt.Sprintf("$to_%d$from_%d$", to, from))
}

var endsOfStreamsPrefix = []byte("$ends_of_streams$")

func (rs *ShuffleReceiver) Next(ctx context.Context) (*Record, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	streamPrefixedTx := tx.WithPrefix(rs.streamID.AsPrefix())
	endOfStreamsMap := storage.NewMap(streamPrefixedTx.WithPrefix(endsOfStreamsPrefix))
	watermarksMap := storage.NewMap(streamPrefixedTx.WithPrefix(watermarksPrefix))

	// We want to randomize the order so we don't always read from the first input if records are available.
	sourceOrder := rand.Perm(rs.sourcePartitionCount)

	changeSubscriptions := make([]*storage.Subscription, rs.sourcePartitionCount)

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

		sourcePartitionOutputQueue := NewOutputQueue(
			tx.WithPrefix(rs.shuffleID.AsPrefix()).WithPrefix(getQueuePrefix(sourcePartition, rs.partition)),
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

func (rs *ShuffleReceiver) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	streamPrefixedTx := tx.WithPrefix(rs.streamID.AsPrefix())
	watermarksMap := storage.NewMap(streamPrefixedTx.WithPrefix(watermarksPrefix))

	var earliestWatermark time.Time

	for sourcePartition := 0; sourcePartition < rs.sourcePartitionCount; sourcePartition++ {
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

func (rs *ShuffleReceiver) Close(ctx context.Context, storage storage.Storage) error {
	if err := storage.DropAll(rs.streamID.AsPrefix()); err != nil {
		return errors.Wrap(err, "couldn't clear storage with streamID prefix")
	}

	return nil
}

// ShuffleSender is used to send data to a shuffle from a given partition.
type ShuffleSender struct {
	streamID             *StreamID
	shuffleID            *ShuffleID
	shuffleStrategy      ShuffleStrategy
	outputPartitionCount int
	partition            int

	sent int
}

func NewShuffleSender(streamID *StreamID, shuffleID *ShuffleID, shuffleStrategy ShuffleStrategy, outputPartitionCount int, partition int) *ShuffleSender {
	return &ShuffleSender{
		streamID:             streamID,
		shuffleID:            shuffleID,
		shuffleStrategy:      shuffleStrategy,
		outputPartitionCount: outputPartitionCount,
		partition:            partition,
	}
}

func (node *ShuffleSender) ReadyForMore(ctx context.Context, tx storage.StateTransaction) error {
	return nil
}

func (node *ShuffleSender) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, record *Record) error {
	outputPartition, err := node.shuffleStrategy.CalculatePartition(ctx, record, node.outputPartitionCount)
	if err != nil {
		return errors.Wrap(err, "couldn't calculate output partition to send record to")
	}

	outputPartitionOutputQueue := NewOutputQueue(
		tx.WithPrefix(node.shuffleID.AsPrefix()).WithPrefix(getQueuePrefix(node.partition, outputPartition)),
	)

	if err := outputPartitionOutputQueue.Push(ctx, &QueueElement{
		Type: &QueueElement_Record{
			Record: record,
		},
	}); err != nil {
		return errors.Wrapf(err, "couldn't send record to output queue for input partition %d, output partition %d", node.partition, outputPartition)
	}

	return nil
}

func (node *ShuffleSender) sendToAllOutputPartitions(ctx context.Context, tx storage.StateTransaction, element *QueueElement) error {
	for outputPartition := 0; outputPartition < node.outputPartitionCount; outputPartition++ {
		outputPartitionOutputQueue := NewOutputQueue(
			tx.WithPrefix(node.shuffleID.AsPrefix()).WithPrefix(getQueuePrefix(node.partition, outputPartition)),
		)

		if err := outputPartitionOutputQueue.Push(ctx, element); err != nil {
			return errors.Wrapf(err, "couldn't send to output queue for input partition %d, output partition %d", node.partition, outputPartition)
		}
	}

	return nil
}

func (node *ShuffleSender) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	watermarkTimestamp, err := ptypes.TimestampProto(watermark)
	if err != nil {
		return errors.Wrap(err, "couldn't convert watermark to proto timestamp")
	}

	if err := node.sendToAllOutputPartitions(ctx, tx, &QueueElement{
		Type: &QueueElement_Watermark{
			Watermark: watermarkTimestamp,
		},
	}); err != nil {
		return errors.Wrap(err, "couldn't send watermark")
	}

	return nil
}

func (node *ShuffleSender) TriggerKeys(ctx context.Context, tx storage.StateTransaction, batchSize int) (int, error) {
	return 0, nil
}

func (node *ShuffleSender) MarkEndOfStream(ctx context.Context, tx storage.StateTransaction) error {
	if err := node.sendToAllOutputPartitions(ctx, tx, &QueueElement{
		Type: &QueueElement_EndOfStream{
			EndOfStream: true,
		},
	}); err != nil {
		return errors.Wrap(err, "couldn't send end of stream")
	}

	return nil
}

func (node *ShuffleSender) MarkError(ctx context.Context, tx storage.StateTransaction, err error) error {
	if err := node.sendToAllOutputPartitions(ctx, tx, &QueueElement{
		Type: &QueueElement_Error{
			Error: err.Error(),
		},
	}); err != nil {
		return errors.Wrap(err, "couldn't send error")
	}

	return nil
}

func (node *ShuffleSender) Close(ctx context.Context, storage storage.Storage) error {
	if err := storage.DropAll(node.streamID.AsPrefix()); err != nil {
		return errors.Wrap(err, "couldn't clear storage with streamID prefix")
	}

	return nil
}

func (node *ShuffleSender) Next(ctx context.Context, tx storage.StateTransaction) (*Record, error) {
	panic("can't get next record from shuffle sender")
}

func (node *ShuffleSender) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	panic("can't get watermark from shuffle sender")
}

type NextShuffleMetadataChange struct {
	ShuffleIDAddSuffix string
	Partition          int
	Source             Node
}

func NewNextShuffleMetadataChange(shuffleIDAddSuffix string, partition int, source Node) *NextShuffleMetadataChange {
	return &NextShuffleMetadataChange{
		ShuffleIDAddSuffix: shuffleIDAddSuffix,
		Partition:          partition,
		Source:             source,
	}
}

func (n *NextShuffleMetadataChange) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, *ExecutionOutput, error) {
	pipelineMetadata := ctx.Value(pipelineMetadataContextKey{}).(PipelineMetadata)
	newPipelineMetadata := PipelineMetadata{
		NextShuffleID: NewShuffleID(pipelineMetadata.NextShuffleID.Id + n.ShuffleIDAddSuffix),
		Partition:     n.Partition,
	}

	newCtx := context.WithValue(ctx, pipelineMetadataContextKey{}, newPipelineMetadata)

	return n.Source.Get(newCtx, variables, streamID)
}
