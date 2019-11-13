package execution

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
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
		first:       firstRecordStream,
		second:      secondRecordStream,
		firstClosed: false,
	}, nil
}

type UnifiedStream struct {
	first, second RecordStream
	firstClosed   bool
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
	for !node.firstClosed {
		firstRecord, err := node.first.Next()
		if err != nil {
			if err == ErrEndOfStream {
				node.firstClosed = true
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

func (node *UnifiedStream) Poll(ctx context.Context) (*MetaRecord, error) {
	if !node.firstClosed {
		r, err := node.first.Poll(ctx)
		if err != nil {
			if err == ErrNoMetarecordPending {
				return nil, ErrNoMetarecordPending
			}
			return nil, errors.Wrap(err, "couldn't get metarecord from first node")
		}
		return r, nil
	}

	r, err := node.second.Poll(ctx)
	if err != nil {
		if err == ErrNoMetarecordPending {
			return nil, ErrNoMetarecordPending
		}
		return nil, errors.Wrap(err, "couldn't get metarecord from second node")
	}
	return r, nil
}
