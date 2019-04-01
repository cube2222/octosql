package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
	"os"
)

const limitAll int = -42
const offsetNone int = 0

type Limit struct {
	data   Node
	limit  Node
	offset Node
}

func NewLimit(data, limit, offset Node) *Limit {
	return &Limit{data: data, limit: limit, offset: offset}
}

func (node *Limit) Get(variables octosql.Variables) (RecordStream, error) {
	var limitVal, offsetVal = limitAll, offsetNone

	dataStream, err := node.data.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get data record stream")
	}

	if node.limit != nil {
		limitStream, err := node.limit.Get(variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get limit record stream")
		}

		record, err := limitStream.Next()
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get limit expression value")
		}
		if len(record.fieldNames) != 1 {
			return nil, errors.Wrap(err, "more than one column in limit expression query")
		}

		// some asserting that good type of variable ... (positive int or string "ALL" if I decide to support it)
		limitVal = record.data[0]

		_, err = limitStream.Next()
		if err != ErrEndOfStream {
			return nil, errors.Wrap(err, "limit expression query has more than one row")
		}
	}

	if node.offset != nil {
		offsetStream, err := node.offset.Get(variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get offset record stream")
		}

		record, err := offsetStream.Next()
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get offset expression value")
		}
		if len(record.fieldNames) != 1 {
			return nil, errors.Wrap(err, "more than one column in offset expression query")
		}

		// some asserting that good type of variable ...
		offsetVal = record.data[0]

		_, err = offsetStream.Next()
		if err != ErrEndOfStream {
			return nil, errors.Wrap(err, "offset expression query has more than one row")
		}
	}

	ls := &LimitedStream{
		variables: variables,
		rs:        dataStream,
		limit:     limitVal,
		finished:  false,
	}

	for ; offsetVal > 0; offsetVal-- {
		_, err :=
	}

	return ls, nil
}

type LimitedStream struct {
	rs        RecordStream
	variables octosql.Variables
	limit     int
	finished  bool
}

func (node *LimitedStream) Next() (*Record, error) {
	if (!node.finished) {
		for node.limit != 0 {
			record, err := node.rs.Next()
			if err != nil {
				if err == ErrEndOfStream {
					node.finished = true
					return nil, ErrEndOfStream
				}
				return nil, errors.Wrap(err, "couldn't get node record")
			}
			node.limit--
			node.finished = (node.limit  0)
			return record, nil
		}
	}
	return nil, ErrEndOfStream
}
