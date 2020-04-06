package execution

import (
	"context"
	"fmt"
	"time"

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

	receiver := NewShuffleReceiver(streamID, pipelineMetadata.NextShuffleID, pipelineMetadata.Partition)
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
	streamID  *StreamID
	shuffleID *ShuffleID
	partition int
}

func NewShuffleReceiver(streamID *StreamID, shuffleID *ShuffleID, partition int) *ShuffleReceiver {
	return &ShuffleReceiver{
		streamID:  streamID,
		shuffleID: shuffleID,
		partition: partition,
	}
}

func (s *ShuffleReceiver) Next(ctx context.Context) (*Record, error) {
	panic("implement me")
}

func (s *ShuffleReceiver) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	panic("implement me")
}

func (s *ShuffleReceiver) Close() error {
	panic("implement me")
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
