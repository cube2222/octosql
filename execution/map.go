package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type Map struct {
	expressions []NamedExpression
	source      Node
	keep        bool
}

func NewMap(expressions []NamedExpression, child Node, keep bool) *Map {
	return &Map{expressions: expressions, source: child, keep: keep}
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
		keep:        node.keep,
	}, nil
}

type MappedStream struct {
	expressions []NamedExpression
	variables   octosql.Variables
	source      RecordStream
	keep        bool
}

func (stream *MappedStream) Close() error {
	err := stream.source.Close()
	if err != nil {
		return errors.Wrap(err, "Couldn't close underlying stream")
	}

	return nil
}

func (stream *MappedStream) Next() (*Record, error) {
	srcRecord, err := stream.source.Next()
	if err != nil {
		if err == ErrEndOfStream {
			return nil, ErrEndOfStream
		}
		return nil, errors.Wrap(err, "couldn't get source record")
	}

	if !srcRecord.IsDataRecord() {
		return srcRecord, nil
	}

	variables, err := stream.variables.MergeWith(srcRecord.AsVariables())
	if err != nil {
		return nil, errors.Wrap(err, "couldn't merge given variables with record variables")
	}

	fieldNames := make([]octosql.VariableName, 0)
	outValues := make(map[octosql.VariableName]octosql.Value)
	for _, expr := range stream.expressions {
		fieldNames = append(fieldNames, expr.Name())

		value, err := expr.ExpressionValue(variables)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't get expression %v", expr.Name())
		}
		outValues[expr.Name()] = value
	}

	if stream.keep {
		for _, name := range srcRecord.fieldNames {
			if _, ok := outValues[name]; !ok {
				fieldNames = append(fieldNames, name)
				outValues[name] = srcRecord.Value(name)
			}
		}
	}

	return NewRecord(fieldNames, outValues, WithMetadataFrom(srcRecord)), nil
}
