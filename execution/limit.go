package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type Limit struct {
	data      Node
	limitExpr Expression
}

func NewLimit(data Node, limit Expression) *Limit {
	return &Limit{data: data, limitExpr: limit}
}

func (node *Limit) Get(variables octosql.Variables) (RecordStream, error) {
	dataStream, err := node.data.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get data RecordStream")
	}

	exprVal, err := extractSingleValue(node.limitExpr, variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't extract value from limit subexpression")
	}

	limitVal, ok := exprVal.(int)
	if !ok {
		return nil, errors.New("limit value not int")
	}
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

func (node *LimitedStream) Next() (*Record, error) {
	for node.limit > 0 {
		node.limit--
		record, err := node.rs.Next()
		if err != nil {
			if err == ErrEndOfStream {
				node.limit = 0
				return nil, ErrEndOfStream
			}
			return nil, errors.Wrap(err, "LimitedStream: couldn't get record")
		}
		return record, nil
	}

	return nil, ErrEndOfStream
}
