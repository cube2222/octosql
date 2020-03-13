package tvf

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/streaming/storage"
)

type Tumble struct {
	source       execution.Node
	timeField    octosql.VariableName
	windowLength execution.Expression
	offset       execution.Expression
}

func NewTumble(source execution.Node, timeField octosql.VariableName, windowLength, offset execution.Expression) *Tumble {
	return &Tumble{
		source:       source,
		timeField:    timeField,
		windowLength: windowLength,
		offset:       offset,
	}
}

func (r *Tumble) Document() docs.Documentation {
	return docs.Section(
		"tumble",
		docs.Body(
			docs.Section("Calling", docs.List(docs.Text("tumble(source => \\<Source\\>, time_field => \\<Descriptor\\>, window_length => \\<interval\\>, offset => \\<interval\\>)"))),
			docs.Section("Description", docs.Text("Adds window_start and window_end of the record, based on which window the time_field value falls into. The source may be specified as a subquery or as TABLE(tablename), and the time_field should be specified as DESCRIPTOR(field_name).")),
		),
	)
}

func (r *Tumble) Get(ctx context.Context, variables octosql.Variables, streamID *execution.StreamID) (execution.RecordStream, *execution.ExecutionOutput, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	sourceStreamID, err := execution.GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakePhantom())
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source stream ID")
	}

	source, execOutput, err := r.source.Get(ctx, variables, sourceStreamID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source")
	}

	duration, err := r.windowLength.ExpressionValue(ctx, variables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get window length")
	}
	if duration.GetType() != octosql.TypeDuration {
		return nil, nil, errors.Errorf("invalid tumble duration: %v", duration)
	}

	offset, err := r.offset.ExpressionValue(ctx, variables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get window offset")
	}
	if offset.GetType() != octosql.TypeDuration {
		return nil, nil, errors.Errorf("invalid tumble offset: %v", offset)
	}

	return &TumbleStream{
		source:       source,
		timeField:    r.timeField,
		windowLength: duration.AsDuration(),
		offset:       offset.AsDuration(),
	}, execOutput, nil
}

type TumbleStream struct {
	source       execution.RecordStream
	timeField    octosql.VariableName
	windowLength time.Duration
	offset       time.Duration
}

func (s *TumbleStream) Next(ctx context.Context) (*execution.Record, error) {
	srcRecord, err := s.source.Next(ctx)
	if err != nil {
		if err == execution.ErrEndOfStream {
			return nil, execution.ErrEndOfStream
		}
		return nil, errors.Wrap(err, "couldn't get source record")
	}

	timeValue := srcRecord.Value(s.timeField)
	if timeValue.GetType() != octosql.TypeTime {
		return nil, fmt.Errorf("couldn't get time field '%v' as time, got: %v", s.timeField.String(), srcRecord.Value(s.timeField).Show())
	}

	windowStart := timeValue.AsTime().Add(-1 * s.offset).Truncate(s.windowLength).Add(s.offset)
	windowEnd := windowStart.Add(s.windowLength)

	fields := make([]octosql.VariableName, len(srcRecord.Fields()), len(srcRecord.Fields())+2)
	values := make([]octosql.Value, len(srcRecord.Fields()), len(srcRecord.Fields())+2)

	for i, field := range srcRecord.Fields() {
		fields[i] = field.Name
		values[i] = srcRecord.Value(field.Name)
	}

	fields = append(fields, octosql.NewVariableName("window_start"), octosql.NewVariableName("window_end"))
	values = append(values, octosql.MakeTime(windowStart), octosql.MakeTime(windowEnd))

	newRecord := execution.NewRecordFromSlice(fields, values, execution.WithMetadataFrom(srcRecord), execution.WithEventTimeField("window_end"))

	return newRecord, nil
}

func (s *TumbleStream) Close() error {
	return s.source.Close()
}
