package execution

import (
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"

	"context"

	"github.com/pkg/errors"
)

type UnionAll struct {
	first, second Node
}

func NewUnionAll(first, second Node) *UnionAll {
	return &UnionAll{first: first, second: second}
}

func (node *UnionAll) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	prefixedTx := tx.WithPrefix(streamID.AsPrefix())

	firstSourceStreamID, err := GetSourceStreamID(prefixedTx, octosql.MakeInt(1))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get first source stream ID")
	}
	secondSourceStreamID, err := GetSourceStreamID(prefixedTx, octosql.MakeInt(2))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get first source stream ID")
	}

	firstRecordStream, err := node.first.Get(ctx, variables, firstSourceStreamID)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get first record stream")
	}
	secondRecordStream, err := node.second.Get(ctx, variables, secondSourceStreamID)
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

func (node *UnifiedStream) Next(ctx context.Context) (*Record, error) {
	for {
		firstRecord, err := node.first.Next(ctx)
		if err != nil {
			if err == ErrEndOfStream {
				break
			}
			return nil, errors.Wrap(err, "couldn't get first node record")
		}
		return firstRecord, nil
	}
	for {
		secondRecord, err := node.second.Next(ctx)
		if err != nil {
			if err == ErrEndOfStream {
				return nil, ErrEndOfStream
			}
			return nil, errors.Wrap(err, "couldn't get second node record")
		}
		return secondRecord, nil
	}
}
