package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type Limit struct {
	data   Node
	limit  Expression
	offset Expression
}

const offsetNone, limitAll = 0, -1

func NewLimit(data Node, limit, offset Expression) *Limit {
	return &Limit{data: data, limit: limit, offset: offset}
}

func extractSingleValue(e Expression, name string, variables octosql.Variables) (value interface{}, err error) {
	switch exprType := e.(type) {
	case *Variable:
		val, err := exprType.ExpressionValue(variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get "+name+" variable value")
		}
		return val, nil // parser set Limit's limit|offset to NewConstant(nil) if there had been no limit|offset in original SQL qiuery
	case *NodeExpression:
		val, err := exprType.ExpressionValue(variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get "+name+" nodeExpression value")
		}
		if val == nil {
			return nil, errors.New(name + " nodeExpression empty")
		}
		switch val.(type) {
		case []Record:
			return nil, errors.New(name + " nodeExpression has multiple rows")
		case Record:
			return nil, errors.New(name + " nodeExpression has multiple columns")
		default:
			return val, nil
		}
	default:
		return nil, errors.New("unexpected type in type-switch @ execution/limit.go")
	}
}

func (node *Limit) Get(variables octosql.Variables) (RecordStream, error) {
	var limit, offset = limitAll, offsetNone

	dataStream, err := node.data.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get data record stream")
	}

	limitVal, err := extractSingleValue(node.limit, "limit", variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't extract value from limit subexpression")
	}

	if limitVal != nil { // means no "LIMIT" in original SQL query
		val, ok := limitVal.(int)
		if !ok {
			return nil, errors.New("limit value not convertible to int")
		}
		if val < 0 {
			return nil, errors.New("negative limit value")
		}
		limit = val
	}

	offsetVal, err := extractSingleValue(node.offset, "offset", variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't extract value from offset subexpression")
	}

	if offsetVal != nil { // means no "OFFSET" in original SQL query
		val, ok := offsetVal.(int)
		if !ok {
			return nil, errors.New("offset value not convertible to int")
		}
		if val < 0 {
			return nil, errors.New("negative offset value")
		}
		offset = val
	}

	ls := &limitedStream{
		rs:    dataStream,
		limit: limit,
	}

	for ; offset > 0; offset-- {
		_, err := ls.Next()
		if err != nil {
			if err == ErrEndOfStream {
				return ls, nil
			}
			return nil, errors.Wrap(err, "couldn't read record from limitedStream")
		}
	}

	return ls, nil
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
