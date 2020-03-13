package execution

import (
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"

	"context"

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

func (node *Map) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, *ExecutionOutput, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	sourceStreamID, err := GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakePhantom())
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source stream ID")
	}

	recordStream, execOutput, err := node.source.Get(ctx, variables, sourceStreamID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get record stream")
	}

	return &MappedStream{
		expressions: node.expressions,
		variables:   variables,
		source:      recordStream,
		keep:        node.keep,
	}, execOutput, nil
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

func (stream *MappedStream) Next(ctx context.Context) (*Record, error) {
	srcRecord, err := stream.source.Next(ctx)
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
	outValues := make(map[octosql.VariableName]octosql.Value)
	for _, expr := range stream.expressions {
		expressionName := expr.Name(variables)

		switch expr := expr.(type) {
		case *StarExpression:
			for _, name := range expressionName {
				value, _ := variables.Get(name) //TODO: err is always nil, handle it anyways?

				outValues[name] = value
			}

			fieldNames = append(fieldNames, expressionName...)

		default: // anything else than a star expression
			if len(expressionName) != 1 {
				return nil, errors.Errorf("expected exactly one expressionName for NamedExpression in map, got %v", len(expressionName))
			}

			actualName := expressionName[0]

			fieldNames = append(fieldNames, actualName)

			value, err := expr.ExpressionValue(ctx, variables)
			if err != nil {
				return nil, errors.Wrapf(err, "couldn't get expression %v", actualName)
			}
			outValues[actualName] = value
		}
	}

	if stream.keep {
		for _, name := range srcRecord.GetVariableNames() {
			if _, ok := outValues[name]; !ok {
				fieldNames = append(fieldNames, name)
				outValues[name] = srcRecord.Value(name)
			}
		}
	}

	return NewRecord(fieldNames, outValues, WithMetadataFrom(srcRecord)), nil
}
