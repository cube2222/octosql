package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type UnionAll struct {
	first, second Node
}

func NewUnionAll(first, second Node) *UnionAll {
	return &UnionAll{first: first, second: second}
}

func (node *UnionAll) Get(variables octosql.Variables) (RecordStream, error) {
	firstRecordStream, err := node.first.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get first record stream")
	}
	secondRecordStream, err := node.second.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get second record stream")
	}

	return &UnifiedStream{
		first:  firstRecordStream,
		second: secondRecordStream,
	}, nil
}

type UnifiedStream struct {
	first, second RecordStream
}

func (node *UnifiedStream) Close() error {
	err := node.first.Close()
	if err != nil {
		return errors.Wrap(err, "Couldn't close first underlying stream")
	}

	err = node.second.Close()
	if err != nil {
		return errors.Wrap(err, "Couldn't close second underlying stream")
	}

	return nil
}

func (node *UnifiedStream) Next() (*Record, error) {
	for {
		firstRecord, err := node.first.Next()
		if err != nil {
			if err == ErrEndOfStream {
				break
			}
			return nil, errors.Wrap(err, "couldn't get first node record")
		}
		return firstRecord, nil
	}
	for {
		secondRecord, err := node.second.Next()
		if err != nil {
			if err == ErrEndOfStream {
				return nil, ErrEndOfStream
			}
			return nil, errors.Wrap(err, "couldn't get second node record")
		}
		return secondRecord, nil
	}
}
