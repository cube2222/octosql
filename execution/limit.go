package execution

import (
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"

	"context"

	"github.com/pkg/errors"
)

type Limit struct {
	data      Node
	limitExpr Expression
}

func NewLimit(data Node, limit Expression) *Limit {
	return &Limit{data: data, limitExpr: limit}
}

func (node *Limit) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	sourceStreamID, err := GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakePhantom())
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get source stream ID")
	}

	dataStream, err := node.data.Get(ctx, variables, sourceStreamID)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get data RecordStream")
	}

	exprVal, err := node.limitExpr.ExpressionValue(ctx, variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't extract value from limit subexpression")
	}

	if exprVal.GetType() != octosql.TypeInt {
		return nil, errors.New("limit value not int")
	}
	limitVal := exprVal.AsInt()
	if limitVal < 0 {
		return nil, errors.New("negative limit value")
	}

	return newLimitedStream(dataStream, limitVal), nil
}

func newLimitedStream(rs RecordStream, limit int) *LimitedStream {
	return &LimitedStream{
		rs:    rs,
		limit: limit,
	}
}

type LimitedStream struct {
	rs    RecordStream
	limit int
}

func (node *LimitedStream) Close() error {
	err := node.rs.Close()
	if err != nil {
		return errors.Wrap(err, "Couldn't close underlying stream")
	}

	return nil
}

func (node *LimitedStream) Next(ctx context.Context) (*Record, error) {
	if node.limit > 0 {
		record, err := node.rs.Next(ctx)
		if err != nil {
			if err == ErrEndOfStream {
				node.limit = 0
				return nil, ErrEndOfStream
			}
			return nil, errors.Wrap(err, "LimitedStream: couldn't get record")
		}
		node.limit--
		if node.limit == 0 {
			node.rs.Close()
		}
		return record, nil
	}

	return nil, ErrEndOfStream
}
