package execution

import (
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"

	"context"

	"github.com/pkg/errors"
)

type Offset struct {
	data       Node
	offsetExpr Expression
}

func NewOffset(data Node, offsetExpr Expression) *Offset {
	return &Offset{data: data, offsetExpr: offsetExpr}
}

func (node *Offset) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	sourceStreamID, err := GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakePhantom())
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get source stream ID")
	}

	dataStream, err := node.data.Get(ctx, variables, sourceStreamID)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get data record stream")
	}

	exprVal, err := node.offsetExpr.ExpressionValue(ctx, variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't extract value from offset subexpression")
	}

	if exprVal.GetType() != octosql.TypeInt {
		return nil, errors.New("offset value not int")

	}
	offsetVal := exprVal.AsInt()
	if offsetVal < 0 {
		return nil, errors.New("negative offset value")
	}

	for ; offsetVal > 0; offsetVal-- {
		_, err := dataStream.Next(ctx)
		if err != nil {
			if err == ErrEndOfStream {
				return dataStream, nil
			}
			return nil, errors.Wrap(err, "couldn't read record from RecordStream")
		}
	}

	return dataStream, nil
}
