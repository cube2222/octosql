package execution

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type Requalifier struct {
	qualifier string
	source    Node
}

func NewRequalifier(qualifier string, child Node) *Requalifier {
	return &Requalifier{qualifier: qualifier, source: child}
}

func (node *Requalifier) Get(variables octosql.Variables) (RecordStream, error) {
	recordStream, err := node.source.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get record stream")
	}

	return &RequalifiedStream{
		qualifier: node.qualifier,
		variables: variables,
		source:    recordStream,
	}, nil
}

type RequalifiedStream struct {
	qualifier string
	variables octosql.Variables
	source    RecordStream
}

// TODO: Do table name validation on logical -> physical plan transformation
var simpleQualifierMatcher = regexp.MustCompile("[a-zA-Z0-9-_]+")

func (stream *RequalifiedStream) Close() error {
	err := stream.source.Close()
	if err != nil {
		return errors.Wrap(err, "Couldn't close underlying stream")
	}

	return nil
}

func (stream *RequalifiedStream) Next() (*Record, error) {
	record, err := stream.source.Next()
	if err != nil {
		if err == ErrEndOfStream {
			return nil, ErrEndOfStream
		}
		return nil, errors.Wrap(err, "couldn't get source record")
	}
	oldFields := record.Fields()

	fields := make([]octosql.VariableName, len(record.Fields()))
	values := make(map[octosql.VariableName]interface{})
	for i := range oldFields {
		name := string(oldFields[i].Name)
		if dotIndex := strings.Index(name, "."); dotIndex != -1 {
			if simpleQualifierMatcher.MatchString(name[:dotIndex]) {
				name = name[dotIndex+1:]
			}
		}
		qualifiedName := octosql.VariableName(fmt.Sprintf("%s.%s", stream.qualifier, name))

		fields[i] = qualifiedName
		values[qualifiedName] = record.Value(oldFields[i].Name)
	}

	return NewRecord(fields, values), nil
}
