package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type Limit struct {
	data      Node
	limitExpr Expression
}

const limitAll = -2137

func NewLimit(data Node, limit Expression) *Limit {
	return &Limit{data: data, limitExpr: limit}
}

func extractSingleValue(e Expression, variables octosql.Variables) (value interface{}, err error) {
	switch exprType := e.(type) {
	case *Variable:
		val, err := exprType.ExpressionValue(variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get variable value")
		}
		return val, nil // parser set Limit's limit|offset to NewConstant(nil) if there had been no limit|offset in original SQL qiuery
	case *NodeExpression:
		val, err := exprType.ExpressionValue(variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get nodeExpression value")
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
	default:
		return nil, errors.New("unexpected type in type-switch @ execution/limit.go")
	}
}

func (node *Limit) Get(variables octosql.Variables) (RecordStream, error) {
	var limitVal = limitAll

	dataStream, err := node.data.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get data record stream")
	}

	exprVal, err := extractSingleValue(node.limitExpr, variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't extract value from limit subexpression")
	}

	if exprVal != nil { // otherwise no "LIMIT" in original SQL query
		val, ok := exprVal.(int)
		if !ok {
			return nil, errors.New("limit value not convertible to int")
		}
		if val < 0 {
			return nil, errors.New("negative limit value")
		}
		limitVal = val
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
	for node.limit != 0 {
		record, err := node.rs.Next()
		if err != nil {
			if err == ErrEndOfStream {
				node.limit = 0
				return nil, ErrEndOfStream
			}
			return nil, errors.Wrap(err, "couldn't get limitedStream's record")
		}
		if node.limit > 0 { // LIMIT not set
			node.limit--
		}
		return record, nil
	}

	return nil, ErrEndOfStream
}
