package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type Map struct {
	expressions []NamedExpression
	source      Node
}

func NewMap(expressions []NamedExpression, child Node) *Map {
	return &Map{expressions: expressions, source: child}
}

func (node *Map) Get(variables octosql.Variables) (RecordStream, error) {
	recordStream, err := node.source.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get record stream")
	}

	return &MappedStream{
		expressions: node.expressions,
		variables:   variables,
		source:      recordStream,
	}, nil
}

type MappedStream struct {
	expressions []NamedExpression
	variables   octosql.Variables
	source      RecordStream
}

func (stream *MappedStream) Next() (*Record, error) {
	srcRecord, err := stream.source.Next()
	if err != nil {
		if err == ErrEndOfStream {
			return nil, ErrEndOfStream
		}
		return nil, errors.Wrap(err, "couldn't get source record")
	}

	variables, err := stream.variables.MergeWith(srcRecord.AsVariables())
	if err != nil {
		return nil, errors.Wrap(err, "couldn't merge given variables with record variables")
	}

	fieldNames := make([]octosql.VariableName, 0)
	outValues := make(map[octosql.VariableName]interface{})
	for _, expr := range stream.expressions {
		fieldNames = append(fieldNames, expr.Name())

		value, err := extractSingleValue(expr, variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't extract single value")
		}
		outValues[expr.Name()] = value
	}

	return NewRecord(fieldNames, outValues), nil
}
