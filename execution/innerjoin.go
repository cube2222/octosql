package execution

import (
	"github.com/cube2222/octosql"
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

func (node *InnerJoin) Get(variables octosql.Variables) (RecordStream, error) {
	recordStream, err := node.source.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get record stream")
	}

	return &InnerJoinedStream{
		joiner:          NewJoiner(node.prefetchCount, variables, recordStream, node.joined),
		curRecord:       nil,
		curJoinedStream: nil,
	}, nil
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

func (stream *InnerJoinedStream) Next() (*Record, error) {
	for {
		if stream.curRecord == nil {
			var err error
			stream.curRecord, stream.curJoinedStream, err = stream.joiner.GetNextRecord()
			if err != nil {
				if err == ErrEndOfStream {
					return nil, ErrEndOfStream
				}
				return nil, errors.Wrap(err, "couldn't get next source record with joined stream from joiner")
			}
			if !stream.curRecord.IsDataRecord() {
				out := stream.curRecord
				stream.curRecord = nil
				stream.curJoinedStream = nil
				return out, nil
			}
		}

		joinedRecord, err := stream.curJoinedStream.Next()
		if err != nil {
			if err == ErrEndOfStream {
				stream.curRecord = nil
				stream.curJoinedStream = nil
				continue
			}
			return nil, errors.Wrap(err, "couldn't get joined record")
		}
		if !joinedRecord.IsDataRecord() {
			return joinedRecord, nil
		}

		fields := stream.curRecord.fieldNames
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
