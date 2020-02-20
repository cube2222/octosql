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

var limitPrefix = []byte("$limit$")

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

	limit, err := node.limitExpr.ExpressionValue(ctx, variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't extract value from limit subexpression")
	}

	if limit.GetType() != octosql.TypeInt {
		return nil, errors.New("limit value not int")
	}
	if limit.AsInt() < 0 {
		return nil, errors.New("negative limit value")
	}

	limitState := storage.NewValueState(tx.WithPrefix(streamID.AsPrefix()).WithPrefix(limitPrefix))
	err = limitState.Set(&limit)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't set limit state")
	}

	return &LimitedStream{
		rs:       dataStream,
		streamID: streamID,
	}, nil
}

type LimitedStream struct {
	rs       RecordStream
	streamID *StreamID
}

func (node *LimitedStream) Close() error {
	err := node.rs.Close()
	if err != nil {
		return errors.Wrap(err, "couldn't close underlying stream")
	}

	return nil
}

func (node *LimitedStream) Next(ctx context.Context) (*Record, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	limitState := storage.NewValueState(tx.WithPrefix(node.streamID.AsPrefix()).WithPrefix(limitPrefix))

	var limitValue octosql.Value
	err := limitState.Get(&limitValue)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get limit state")
	}
	limit := limitValue.AsInt()

	if limit == 0 {
		return nil, ErrEndOfStream
	}

	record, err := node.rs.Next(ctx)
	if err != nil {
		if err == ErrEndOfStream {
			limitValue = octosql.MakeInt(0)
			err = limitState.Set(&limitValue)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't set new limit state")
			}

			return nil, ErrEndOfStream
		}
		return nil, errors.Wrap(err, "couldn't get record")
	}

	limit--
	if limit == 0 {
		err := node.rs.Close()
		if err != nil {
			return nil, errors.Wrap(err, "couldn't close source stream")
		}
	}

	limitValue = octosql.MakeInt(limit)
	err = limitState.Set(&limitValue)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't set new limit state")
	}
	return record, nil
}
