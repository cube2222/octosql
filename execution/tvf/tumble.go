package tvf

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution"
)

type Tumble struct {
	source       execution.Node
	timeField    octosql.VariableName
	windowLength execution.Expression
}

func NewTumble(source execution.Node, timeField octosql.VariableName, windowLength execution.Expression) *Tumble {
	return &Tumble{
		source:       source,
		timeField:    timeField,
		windowLength: windowLength,
	}
}

// TODO: fixme
func (r *Tumble) Document() docs.Documentation {
	return docs.Section(
		"range",
		docs.Body(
			docs.Section("Calling", docs.List(docs.Text("range(range_start => \\<int\\>, range_end => \\<int\\>)"))),
			docs.Section("Description", docs.Text("Returns a list of numbers from range_start inclusive to range_end exclusive. The field name of the number will be i.")),
		),
	)
}

func (r *Tumble) Get(variables octosql.Variables) (execution.RecordStream, error) {
	source, err := r.source.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get source")
	}

	duration, err := r.windowLength.ExpressionValue(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get window length")
	}
	octoDuration, ok := duration.(octosql.Duration)
	if !ok {
		return nil, errors.Errorf("invalid tumble duration: %v", duration)
	}

	return &TumbleStream{
		source:       source,
		timeField:    r.timeField,
		windowLength: octoDuration,
	}, nil
}

type TumbleStream struct {
	source       execution.RecordStream
	timeField    octosql.VariableName
	windowLength octosql.Duration
}

func (s *TumbleStream) Next() (*execution.Record, error) {
	srcRecord, err := s.source.Next()
	if err != nil {
		if err == execution.ErrEndOfStream {
			return nil, execution.ErrEndOfStream
		}
		return nil, errors.Wrap(err, "couldn't get source record")
	}

	timeValue, ok := srcRecord.Value(s.timeField).(octosql.Time)
	if !ok {
		return nil, fmt.Errorf("couldn't get time field '%v' as time, got: %v", s.timeField.String(), srcRecord.Value(s.timeField))
	}

	windowStart := timeValue.AsTime().Truncate(s.windowLength.AsDuration())
	windowEnd := windowStart.Add(s.windowLength.AsDuration())

	fields := make([]octosql.VariableName, len(srcRecord.Fields()), len(srcRecord.Fields())+2)
	values := make([]octosql.Value, len(srcRecord.Fields()), len(srcRecord.Fields())+2)

	for i, field := range srcRecord.Fields() {
		fields[i] = field.Name
		values[i] = srcRecord.Value(field.Name)
	}

	fields = append(fields, octosql.NewVariableName("window_start"), octosql.NewVariableName("window_end"))
	values = append(values, octosql.MakeTime(windowStart), octosql.MakeTime(windowEnd))

	newRecord := execution.NewRecordFromSlice(fields, values, execution.WithMetadataFrom(srcRecord), execution.WithEventTime(windowEnd))

	return newRecord, nil
}

func (s *TumbleStream) Close() error {
	return s.source.Close()
}
