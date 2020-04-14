package execution

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
	"github.com/pkg/errors"
)

const (
	LEFT  = 0
	RIGHT = 1
)

type StreamJoin struct {
	keys           [][]Expression // keys[0] are the expressions for the left source, keys[1] for the right source
	sources        []Node         // sources[0] = left source, sources[1] = right source
	storage        storage.Storage
	eventTimeField octosql.VariableName
	trigger        TriggerPrototype
}

func NewStreamJoin(leftKey, rightKey []Expression, sources []Node, storage storage.Storage, eventTimeField octosql.VariableName, trigger TriggerPrototype) *StreamJoin {
	return &StreamJoin{
		keys:           [][]Expression{leftKey, rightKey},
		sources:        sources,
		storage:        storage,
		eventTimeField: eventTimeField,
		trigger:        trigger,
	}
}

func (node *StreamJoin) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, *ExecutionOutput, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	sourceStreamID, err := GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakePhantom())
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source stream ID")
	}

	leftStream, leftExec, err := node.sources[LEFT].Get(ctx, variables, streamID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get left source stream in stream join")
	}

	rightStream, rightExec, err := node.sources[RIGHT].Get(ctx, variables, streamID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get right source stream in stream join")
	}

	execOutput := &UnifiedStream{
		sources:          []RecordStream{leftStream, rightStream},
		watermarkSources: []WatermarkSource{leftExec.WatermarkSource, rightExec.WatermarkSource},
		streamID:         sourceStreamID, // TODO: is this right?
	}

	trigger, err := node.trigger.Get(ctx, variables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't create trigger for stream join")
	}

	stream := &JoinedStream{}

	processFunc := &ProcessByKey{
		eventTimeField:  node.eventTimeField,
		trigger:         trigger,
		keyExpressions:  node.keys,
		processFunction: stream,
		variables:       variables,
	}

	streamJoinPullEngine := NewPullEngine(processFunc, node.storage, []RecordStream{leftStream, rightStream}, streamID, execOutput)
	go streamJoinPullEngine.Run(ctx)

	return streamJoinPullEngine, NewExecutionOutput(streamJoinPullEngine), nil
}

type JoinedStream struct{}

func (js *JoinedStream) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, key octosql.Value, record *Record) error {
	return nil
}

func (js *JoinedStream) Trigger(ctx context.Context, tx storage.StateTransaction, key octosql.Value) ([]*Record, error) {
	return nil, nil
}
