package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type Distinct struct {
	child Node
}

func NewDistinct(child Node) *Distinct {
	return &Distinct{child: child}
}

type DistinctStream struct {
	stream    RecordStream
	variables octosql.Variables
	records   map[Record]bool
}

func (node *Distinct) Get(variables octosql.Variables) (RecordStream, error) {
	stream, err := node.child.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get stream for child node in distinct")
	}

	return DistinctStream{
		stream:    stream,
		variables: variables,
		records:   make(map[Record]bool),
	}, nil
}

func (distinctStream DistinctStream) Next() (*Record, error) {
	for {
		record, err := distinctStream.stream.Next()
		if err != nil {
			if err == ErrEndOfStream {
				return nil, err
			}
			return nil, errors.Wrap(err, "couldn't get record from stream in DistinctStream")
		}

		_, alreadyFound := distinctStream.records[*record]
		if !alreadyFound {
			distinctStream.records[*record] = true
			return record, nil
		}
	}
}
