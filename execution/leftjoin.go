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
		variables:       variables,
		source:          recordStream,
		joined:          node.joined,
		curRecord:       nil,
		curJoinedStream: nil,
	}, nil
}

type LeftJoinedStream struct {
	variables       octosql.Variables
	source          RecordStream
	joined          Node
	curRecord       *Record
	curJoinedStream RecordStream
	joinedAnyRecord bool
}

func (stream *LeftJoinedStream) Next() (*Record, error) {
	for {
		if stream.curRecord == nil {
			srcRecord, err := stream.source.Next()
			if err != nil {
				if err == ErrEndOfStream {
					return nil, ErrEndOfStream
				}
				return nil, errors.Wrap(err, "couldn't get source record")
			}

			variables, err := stream.variables.MergeWith(srcRecord.AsVariables())
			if err != nil {
				return nil, errors.Wrap(err, "couldn't merge given variables with source record variables")
			}

			stream.curJoinedStream, err = stream.joined.Get(variables)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get joined stream")
			}

			stream.curRecord = srcRecord
			stream.joinedAnyRecord = false
		}

		joinedRecord, err := stream.curJoinedStream.Next()
		if err != nil {
			// TODO: If there's nothing, then we have to put one record with a null right side
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
