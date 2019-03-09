package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type Filter struct {
	formula Formula
	child   Node
}

func NewFilter(formula Formula, child Node) *Filter {
	return &Filter{formula: formula, child: child}
}

func (node *Filter) Get(variables octosql.Variables) (RecordStream, error) {
	recordStream, err := node.child.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get record stream")
	}

	return &FilteredStream{
		formula:   node.formula,
		variables: variables,
		source:    recordStream,
	}, nil
}

type FilteredStream struct {
	formula   Formula
	variables octosql.Variables
	source    RecordStream
}

func (stream *FilteredStream) Next() (*Record, error) {
	for {
		record, err := stream.source.Next()
		if err != nil {
			if err == ErrEndOfStream {
				return nil, ErrEndOfStream
			}
			return nil, errors.Wrap(err, "couldn't get source record")
		}

		variables, err := stream.variables.MergeWith(record.AsVariables())
		if err != nil {
			return nil, errors.Wrap(err, "couldn't merge given variables with record variables")
		}

		predicate, err := stream.formula.Evaluate(variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't evaluate formula")
		}

		if predicate {
			return record, nil
		}
	}
}
