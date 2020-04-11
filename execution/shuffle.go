package execution

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/pkg/errors"
	"github.com/twmb/murmur3"

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
	outputPartitionCount int
	strategy             ShuffleStrategy
	sources              []Node
}

func NewShuffle(outputPartitionCount int, strategy ShuffleStrategy, sources []Node) *Shuffle {
	return &Shuffle{
		outputPartitionCount: outputPartitionCount,
		strategy:             strategy,
		sources:              sources,
	}
}

func (s *Shuffle) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, *ExecutionOutput, error) {
	pipelineMetadata := ctx.Value(pipelineMetadataContextKey{}).(PipelineMetadata)

	receiver := NewShuffleReceiver(streamID, pipelineMetadata.NextShuffleID, len(s.sources), pipelineMetadata.Partition)
	execOutput := NewExecutionOutput(receiver)
	execOutput.NextShuffles = map[string]ShuffleData{
		pipelineMetadata.NextShuffleID.MapKey(): {
			Shuffle:   s,
			Variables: variables,
			ShuffleID: pipelineMetadata.NextShuffleID,
		},
	}

	return receiver, execOutput, nil
}

func (s *Shuffle) StartSources(ctx context.Context, stateStorage storage.Storage, tx storage.StateTransaction, shuffleID *ShuffleID, variables octosql.Variables) (map[string]ShuffleData, error) {
	nextShuffleIDRaw, err := GetSourceStreamID(tx.WithPrefix(shuffleID.AsPrefix()), octosql.MakeString("next_shuffle_id"))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get next shuffle id")
	}
	nextShuffleID := (*ShuffleID)(nextShuffleIDRaw)

	nextShuffles := make(map[string]ShuffleData)

	for partition, node := range s.sources {
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
		sender := NewShuffleSender(senderStreamID, shuffleID, s.strategy, s.outputPartitionCount, partition)

		// panic("fix stream ID in pull engine")
		log.Printf("starting sender for partitiion %d", partition)
		engine := NewPullEngine(sender, stateStorage, rs, nil, execOutput.WatermarkSource, false) // TODO: Fix streamID

		go engine.Run(ctx)
	}

	return nextShuffles, nil
}

type ShuffleReceiver struct {
	streamID             *StreamID
	shuffleID            *ShuffleID
	sourcePartitionCount int
	partition            int
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

func (rs *ShuffleReceiver) Close() error {
	// TODO: Cleanup
	return nil
}

type ShuffleStrategy interface {
	// Return output partition index based on the record and output partition count.
	CalculatePartition(ctx context.Context, record *Record, outputs int) (int, error)
}

type ShuffleSender struct {
	streamID             *StreamID
	shuffleID            *ShuffleID
	shuffleStrategy      ShuffleStrategy
	outputPartitionCount int
	partition            int
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

	log.Printf("sending record %s from %d to %d", record.Show(), node.partition, outputPartition)

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

func (node *ShuffleSender) Close() error {
	// TODO: cleanup
	return nil
}

func (node *ShuffleSender) Next(ctx context.Context, tx storage.StateTransaction) (*Record, error) {
	panic("can't get next record from shuffle sender")
}

func (node *ShuffleSender) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	panic("can't get watermark from shuffle sender")
}

type KeyHashingStrategy struct {
	variables     octosql.Variables
	keyExpression []Expression
}

func NewKeyHashingStrategy(variables octosql.Variables, keyExpression []Expression) ShuffleStrategy {
	return &KeyHashingStrategy{
		variables:     variables,
		keyExpression: keyExpression,
	}
}

// TODO: The key should really be calculated by the preceding map. Like all group by values.
func (s *KeyHashingStrategy) CalculatePartition(ctx context.Context, record *Record, outputs int) (int, error) {
	variables, err := s.variables.MergeWith(record.AsVariables())
	if err != nil {
		return -1, errors.Wrap(err, "couldn't merge stream variables with record")
	}

	key := make([]octosql.Value, len(s.keyExpression))
	for i := range s.keyExpression {
		key[i], err = s.keyExpression[i].ExpressionValue(ctx, variables)
		if err != nil {
			return -1, errors.Wrapf(err, "couldn't evaluate process key expression with index %v", i)
		}
	}

	hash := murmur3.New32()

	keyTuple := octosql.MakeTuple(key)
	_, err = hash.Write(keyTuple.MonotonicMarshal())
	if err != nil {
		return -1, errors.Wrap(err, "couldn't write to hash")
	}

	return int(hash.Sum32() % uint32(outputs)), nil
}
