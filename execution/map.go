package execution

import (
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"

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

func (stream *MappedStream) Close(ctx context.Context, storage storage.Storage) error {
	if err := stream.source.Close(ctx, storage); err != nil {
		return errors.Wrap(err, "couldn't close underlying stream")
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

	recordVariables := srcRecord.AsVariables()

	variables, err := stream.variables.MergeWith(recordVariables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't merge given variables with record variables")
	}

	fieldNames := make([]octosql.VariableName, 0)
	outValues := make(map[octosql.VariableName]octosql.Value)
	for _, expr := range stream.expressions {
		switch expr := expr.(type) {
		case *StarExpression:
			// A star expression is based only on the incoming record
			expressionName := expr.Fields(recordVariables)
			fieldNames = append(fieldNames, expressionName...)

			for _, name := range expressionName {
				value, err := variables.Get(name)
				if err != nil {
					panic(err)
				}

				outValues[name] = value
			}

		default: // anything else than a star expression
			expressionName := expr.Name()
			fieldNames = append(fieldNames, expressionName)

			value, err := expr.ExpressionValue(ctx, variables)
			if err != nil {
				return nil, errors.Wrapf(err, "couldn't get expression %v", expressionName)
			}
			outValues[expressionName] = value
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
