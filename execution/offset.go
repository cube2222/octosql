package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type Offset struct {
	data       Node
	offsetExpr Expression
}

const offsetNone = 0

func NewOffset(data Node, offsetExpr Expression) *Offset {
	return &Offset{data: data, offsetExpr: offsetExpr}
}

func (node *Offset) Get(variables octosql.Variables) (RecordStream, error) {
	var offsetVal = offsetNone

	dataStream, err := node.data.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get data record stream")
	}

	exprVal, err := extractSingleValue(node.offsetExpr, variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't extract value from offset subexpression")
	}

	if exprVal != nil { // means no "OFFSET" in original SQL query
		val, ok := exprVal.(int)
		if !ok {
			return nil, errors.New("offset value not convertible to int")
		}
		if val < 0 {
			return nil, errors.New("negative offset value")
		}
		offsetVal = val
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
