package execution

import (
	"github.com/cube2222/octosql"

	"context"

	"github.com/pkg/errors"
)

// InnerJoin currently only supports lookup joins.
type InnerJoin struct {
	prefetchCount int
	source        Node
	joined        Node
}

func NewInnerJoin(prefetchCount int, source Node, joined Node) *InnerJoin {
	return &InnerJoin{prefetchCount: prefetchCount, source: source, joined: joined}
}

func (node *InnerJoin) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, *ExecOutput, error) {
	recordStream, execOutput, err := node.source.Get(ctx, variables, streamID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get record stream")
	}

	return &InnerJoinedStream{
		joiner:          NewJoiner(node.prefetchCount, variables, recordStream, node.joined),
		curRecord:       nil,
		curJoinedStream: nil,
	}, execOutput, nil
}

type InnerJoinedStream struct {
	joiner *Joiner

	curRecord       *Record
	curJoinedStream RecordStream
}

func (stream *InnerJoinedStream) Close() error {
	err := stream.joiner.Close()
	if err != nil {
		return errors.Wrap(err, "couldn't close joiner")
	}

	err = stream.curJoinedStream.Close()
	if err != nil {
		return errors.Wrap(err, "Couldn't close joined stream")
	}

	return nil
}

func (stream *InnerJoinedStream) Next(ctx context.Context) (*Record, error) {
	for {
		if stream.curRecord == nil {
			var err error
			stream.curRecord, stream.curJoinedStream, err = stream.joiner.GetNextRecord(ctx)
			if err != nil {
				if err == ErrEndOfStream {
					return nil, ErrEndOfStream
				}
				return nil, errors.Wrap(err, "couldn't get next source record with joined stream from joiner")
			}
		}

		joinedRecord, err := stream.curJoinedStream.Next(ctx)
		if err != nil {
			if err == ErrEndOfStream {
				stream.curRecord = nil
				stream.curJoinedStream = nil
				continue
			}
			return nil, errors.Wrap(err, "couldn't get joined record")
		}

		fields := stream.curRecord.GetVariableNames()
		for _, field := range joinedRecord.Fields() {
			fields = append(fields, field.Name)
		}

		allVariableValues, err := stream.curRecord.AsVariables().MergeWith(joinedRecord.AsVariables())
		if err != nil {
			return nil, errors.Wrap(err, "couldn't merge current record variables with joined record variables")
		}

		return NewRecord(fields, allVariableValues), nil
	}
}
