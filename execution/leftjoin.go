package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

// LeftJoin currently only supports lookup joins.
type LeftJoin struct {
	source Node
	joined Node
}

func NewLeftJoin(source Node, joined Node) *LeftJoin {
	return &LeftJoin{source: source, joined: joined}
}

func (node *LeftJoin) Get(variables octosql.Variables) (RecordStream, error) {
	recordStream, err := node.source.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get record stream")
	}

	return &LeftJoinedStream{
		joiner:          NewJoiner(variables, recordStream, node.joined),
		curRecord:       nil,
		curJoinedStream: nil,
	}, nil
}

type LeftJoinedStream struct {
	joiner *Joiner

	curRecord       *Record
	curJoinedStream RecordStream
	joinedAnyRecord bool
}

func (stream *LeftJoinedStream) Close() error {
	err := stream.joiner.Close()
	if err != nil {
		return errors.Wrap(err, "couldn't close joiner")
	}

	err = stream.curJoinedStream.Close()
	if err != nil {
		return errors.Wrap(err, "couldn't close joined stream")
	}

	return nil
}

func (stream *LeftJoinedStream) Next() (*Record, error) {
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

			stream.joinedAnyRecord = false
		}

		joinedRecord, err := stream.curJoinedStream.Next()
		if err != nil {
			if err == ErrEndOfStream {
				if !stream.joinedAnyRecord {
					toReturn := stream.curRecord
					stream.curRecord = nil
					stream.curJoinedStream = nil
					return toReturn, nil
				}
				stream.curRecord = nil
				stream.curJoinedStream = nil
				continue
			}
			return nil, errors.Wrap(err, "couldn't get joined record")
		}
		stream.joinedAnyRecord = true

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
