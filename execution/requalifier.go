package execution

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

type Requalifier struct {
	qualifier string
	source    Node
}

func NewRequalifier(qualifier string, child Node) *Requalifier {
	return &Requalifier{qualifier: qualifier, source: child}
}

func (node *Requalifier) Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, *ExecutionOutput, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	sourceStreamID, err := GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakePhantom())
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source stream ID")
	}

	recordStream, execOutput, err := node.source.Get(ctx, variables, sourceStreamID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get record stream")
	}

	return &RequalifiedStream{
		qualifier: node.qualifier,
		variables: variables,
		source:    recordStream,
	}, execOutput, nil
}

type RequalifiedStream struct {
	qualifier string
	variables octosql.Variables
	source    RecordStream
}

// TODO: Do table name validation on logical -> physical plan transformation
var simpleQualifierMatcher = regexp.MustCompile("[a-zA-Z0-9-_]+")

func (stream *RequalifiedStream) Close(ctx context.Context, storage storage.Storage) error {
	if err := stream.source.Close(ctx, storage); err != nil {
		return errors.Wrap(err, "couldn't close underlying stream")
	}

	return nil
}

func (stream *RequalifiedStream) Next(ctx context.Context) (*Record, error) {
	record, err := stream.source.Next(ctx)
	if err != nil {
		if err == ErrEndOfStream {
			return nil, ErrEndOfStream
		}
		return nil, errors.Wrap(err, "couldn't get source record")
	}
	oldFields := record.Fields()

	fields := make([]octosql.VariableName, len(record.Fields()))
	values := make(map[octosql.VariableName]octosql.Value)
	for i := range oldFields {
		if oldFields[i].Name.Source() == SystemSource {
			continue
		}
		name := string(oldFields[i].Name)
		if dotIndex := strings.Index(name, "."); dotIndex != -1 {
			if simpleQualifierMatcher.MatchString(name[:dotIndex]) {
				name = name[dotIndex+1:]
			}
		}
		qualifiedName := octosql.NewVariableName(fmt.Sprintf("%s.%s", stream.qualifier, name))

		fields[i] = qualifiedName
		values[qualifiedName] = record.Value(oldFields[i].Name)
	}

	eventTimeField := record.EventTimeField()
	if !eventTimeField.Empty() {
		eventTimeField = octosql.NewVariableName(fmt.Sprintf("%s.%s", stream.qualifier, eventTimeField.Name()))
	}

	out := NewRecord(fields, values, WithMetadataFrom(record), WithEventTimeField(eventTimeField))

	return out, nil
}
