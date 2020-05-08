package execution

import (
	"context"
	"fmt"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/mitchellh/hashstructure"
	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

type Field struct {
	Name octosql.VariableName
}

func (id RecordID) Show() string {
	return id.ID
}

type RecordOption func(stream *Record)

func WithNoUndo() RecordOption {
	return func(r *Record) {
		r.Metadata.Undo = false
	}
}

func WithUndo() RecordOption {
	return func(r *Record) {
		r.Metadata.Undo = true
	}
}

func WithEventTimeField(field octosql.VariableName) RecordOption {
	return func(r *Record) {
		r.Metadata.EventTimeField = field.String()
	}
}

func WithMetadataFrom(base *Record) RecordOption {
	return func(r *Record) {
		r.Metadata = base.Metadata
	}
}

func WithID(id *RecordID) RecordOption {
	return func(rec *Record) {
		rec.Metadata.Id = id
	}
}

func NewRecord(fields []octosql.VariableName, data map[octosql.VariableName]octosql.Value, opts ...RecordOption) *Record {
	dataInner := make([]octosql.Value, len(fields))
	for i := range fields {
		dataInner[i] = data[fields[i]]
	}
	return NewRecordFromSlice(fields, dataInner, opts...)
}

func NewRecordFromSlice(fields []octosql.VariableName, data []octosql.Value, opts ...RecordOption) *Record {
	stringFields := octosql.VariableNamesToStrings(fields)

	pointerData := octosql.GetPointersFromValues(data)

	r := &Record{
		FieldNames: stringFields,
		Data:       pointerData,
		Metadata:   &Metadata{},
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

func NewRecordFromRecord(record *Record, opts ...RecordOption) *Record {
	metadataCopy := *record.Metadata
	r := &Record{
		FieldNames: record.FieldNames,
		Data:       record.Data,
		Metadata:   &metadataCopy,
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

var SystemSource string = "sys"

func SystemField(field string) octosql.VariableName {
	return octosql.NewVariableName(fmt.Sprintf("%s.%s", SystemSource, field))
}

func (r *Record) Value(field octosql.VariableName) octosql.Value {
	if field.Source() == SystemSource {
		switch field.Name() {
		case "id":
			return octosql.MakeString(r.ID().Show())
		case "undo":
			return octosql.MakeBool(r.IsUndo())
		case "event_time_field":
			eventTimeField := r.EventTimeField()
			return octosql.MakeString(eventTimeField.String())
		default:
			return octosql.MakeNull()
		}
	}

	stringField := field.String()

	for i := range r.FieldNames {
		if r.FieldNames[i] == stringField {
			return *r.Data[i]
		}
	}
	return octosql.MakeNull()
}

func (r *Record) Fields() []Field {
	fields := make([]Field, 0)
	for _, fieldName := range r.FieldNames {
		fields = append(fields, Field{
			Name: octosql.NewVariableName(fieldName),
		})
	}

	return fields
}

func (r *Record) ShowFields() []Field {
	fields := r.Fields()

	if len(r.EventTimeField()) > 0 {
		fields = append(fields, Field{
			Name: octosql.NewVariableName("sys.event_time_field"),
		})
	}
	if r.IsUndo() {
		fields = append(fields, Field{
			Name: octosql.NewVariableName("sys.undo"),
		})
	}

	fields = append(fields, Field{
		Name: octosql.NewVariableName("sys.id"),
	})

	return fields
}

func (r *Record) AsVariables() octosql.Variables {
	out := make(octosql.Variables)
	for i := range r.FieldNames {
		out[octosql.NewVariableName(r.FieldNames[i])] = *r.Data[i]
	}
	// out[octosql.NewVariableName("sys.undo")] = octosql.MakeBool(r.IsUndo())
	// out[octosql.NewVariableName("sys.id")] = octosql.MakeString(r.ID().Show())

	return out
}

func (r *Record) AsTuple() octosql.Value {
	return octosql.MakeTuple(octosql.GetValuesFromPointers(r.Data))
}

func (r *Record) Equal(other *Record) bool {
	return proto.Equal(r, other)
}

func (r *Record) Show() string {
	fields := r.ShowFields()
	parts := make([]string, len(fields))
	for i, field := range fields {
		parts[i] = fmt.Sprintf("%s: %s", field.Name, r.Value(field.Name).Show())
	}

	return fmt.Sprintf("{%s}", strings.Join(parts, ", "))
}

func (r *Record) IsUndo() bool {
	if r.Metadata != nil {
		return r.Metadata.Undo
	}

	return false
}

func (r *Record) EventTime() octosql.Value {
	eventVarName := r.EventTimeField()
	return r.Value(eventVarName)
}

func (r *Record) ID() *RecordID {
	if r.Metadata != nil {
		if r.Metadata.Id != nil {
			return r.Metadata.Id
		}
	}

	return &RecordID{}
}

func (r *Record) EventTimeField() octosql.VariableName {
	if r.Metadata != nil {
		return octosql.NewVariableName(r.Metadata.EventTimeField)
	}

	return octosql.NewVariableName("")
}

func (r *Record) GetVariableNames() []octosql.VariableName {
	return octosql.StringsToVariableNames(r.FieldNames)
}

func (r *Record) Hash() (uint64, error) {
	values := octosql.GetValuesFromPointers(r.Data)
	fields := make([]octosql.Value, len(r.FieldNames))

	for i, name := range r.FieldNames {
		fields[i] = octosql.MakeString(name)
	}

	return hashstructure.Hash(append(values, fields...), nil)
}

// GetRandomRecordID can be used to get a new random RecordID.
func NewRecordID(id string) *RecordID {
	return &RecordID{
		ID: id,
	}
}

// NewRecordIDFromStreamIDWithOffset can be used to get a new RecordID deterministically based on the streamID and record offset.
func NewRecordIDFromStreamIDWithOffset(streamID *StreamID, offset int) *RecordID {
	return &RecordID{
		ID: fmt.Sprintf("%s.%d", streamID.Id, offset),
	}
}

// This is a helper function to use a record ID as a storage prefix.
func (id *RecordID) AsPrefix() []byte {
	return []byte("$" + id.ID + "$")
}

func (id *RecordID) MonotonicMarshal() []byte {
	return []byte(id.ID)
}

func (id *RecordID) MonotonicUnmarshal(data []byte) error {
	id.ID = string(data)
	return nil
}

type RecordStream interface {
	Next(ctx context.Context) (*Record, error)
	Close(ctx context.Context, storage storage.Storage) error
}

var ErrEndOfStream = errors.New("end of stream")
