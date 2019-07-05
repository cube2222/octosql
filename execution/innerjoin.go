package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

// InnerJoin currently only supports lookup joins.
type InnerJoin struct {
	source Node
	joined Node
}

func NewInnerJoin(source Node, joined Node) *InnerJoin {
	return &InnerJoin{source: source, joined: joined}
}

func (node *InnerJoin) Get(variables octosql.Variables) (RecordStream, error) {
	recordStream, err := node.source.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get record stream")
	}

	return &InnerJoinedStream{
		variables:       variables,
		source:          recordStream,
		joined:          node.joined,
		curRecord:       nil,
		curJoinedStream: nil,
	}, nil
}

type InnerJoinedStream struct {
	variables       octosql.Variables
	source          RecordStream
	joined          Node
	curRecord       *Record
	curJoinedStream RecordStream
}

func (stream *InnerJoinedStream) Close() error {
	err := stream.source.Close()
	if err != nil {
		return errors.Wrap(err, "Couldn't close source stream")
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
