package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type Offset struct {
	data       Node
	offsetExpr Expression
}

func NewOffset(data Node, offsetExpr Expression) *Offset {
	return &Offset{data: data, offsetExpr: offsetExpr}
}

func (node *Offset) Get(variables octosql.Variables) (RecordStream, error) {
	dataStream, err := node.data.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get data record stream")
	}

	exprVal, err := node.offsetExpr.ExpressionValue(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't extract value from offset subexpression")
	}

	offsetVal, ok := exprVal.(octosql.Int)
	if !ok {
		return nil, errors.New("offset value not int")
	}
	if offsetVal < 0 {
		return nil, errors.New("negative offset value")
	}

	for ; offsetVal > 0; offsetVal-- {
		_, err := dataStream.Next()
		if err != nil {
			if err == ErrEndOfStream {
				return dataStream, nil
			}
			return nil, errors.Wrap(err, "couldn't read record from RecordStream")
		}
	}

	return dataStream, nil
}
