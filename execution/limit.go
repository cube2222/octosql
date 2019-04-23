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

func extractSingleValue(expr Expression, variables octosql.Variables) (value interface{}, err error) {
	nodeExpr, ok := expr.(*NodeExpression)
	if !ok {
		return nil, errors.New("unexpected type of expression")
	}

	val, err := nodeExpr.ExpressionValue(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get expression value")
	}

	if val == nil {
		return nil, errors.New("nodeExpression empty")
	}
	switch val.(type) {
	case []Record:
		return nil, errors.New("nodeExpression has multiple rows")
	case Record:
		return nil, errors.New("nodeExpression has multiple columns")
	default:
		return val, nil
	}
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

	return &limitedStream{
		rs:    dataStream,
		limit: limitVal,
	}, nil
}

type limitedStream struct {
	rs    RecordStream
	limit int
}

func (node *limitedStream) Next() (*Record, error) {
	for node.limit > 0 {
		node.limit--
		record, err := node.rs.Next()
		if err != nil {
			if err == ErrEndOfStream {
				node.limit = 0
				return nil, ErrEndOfStream
			}
			return nil, errors.Wrap(err, "limitedStream: couldn't get record")
		}
		return record, nil
	}

	return nil, ErrEndOfStream
}
