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
	expr, err := e.ExpressionValue(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get "+name+" expression value")
	}
	switch exprType := expr.(type) {
	case Variable:
		val, err := exprType.ExpressionValue(variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get " + name + " variable value")
		}
		return val, nil
	case NodeExpression:
		val, err := exprType.ExpressionValue(variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get " + name + " nodeExpression value")
		}
		if val == nil {
			return nil, errors.New(name + " nodeExpression empty")
		}
		switch valType := val.(type){
		case []Record:
			return nil, errors.New(name + " nodeExpression has multiple rows")
		case Record:
			return nil, errors.New(name + " nodeExpression has multiple columns")
		default:
			return val, nil
		}
	// start debug
	case Node:
		panic("Assert t.type != Node failed (execution/limit.go:extractSingleValue)")
	case Constant:
		panic("Assert t.type != Constant failed (execution/limit.go:extractSingleValue)")
	// end debug*/
	default:
		return nil, errors.New(name + " expression value invalid")
	}
}

// as it is taken out of good Expression, val's type should already be normalized


func (node *Limit) Get(variables octosql.Variables) (RecordStream, error) {
	var limit, offset = limitAll, offsetNone
	var limitVariables, offsetVariables = octosql.NoVariables(), octosql.NoVariables()

	dataStream, err := node.data.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get data record stream")
	}

	limitVal, err := extractSingleValue(node.limit, "limit", variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't extract value from limit subexpression")
	}

	val, err :=

	ls := &limitedStream{
		variables: variables,
		rs:        dataStream,
		limit:     limit,
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
	rs        RecordStream
	variables octosql.Variables
	limit     int
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
		if node.limit > 0 { // LIMIT ALL and such
			node.limit--
		}
		return record, nil
	}

	return nil, ErrEndOfStream
}
