package tvf

import (
	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution"
)

type Tumble struct {
	source    execution.Node
	timeField string
	duration  execution.Expression
}

func NewTumble(source execution.Node, timeField string, duration execution.Expression) *Tumble {
	return &Tumble{
		source:    source,
		timeField: timeField,
		duration:  duration,
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

	duration, err := r.duration.ExpressionValue(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get range duration")
	}
	octoDuration, ok := duration.(octosql.Duration)
	if !ok {
		return nil, errors.Errorf("invalid tumble duration: %v", duration)
	}

	return &TumbleStream{
		source:    source,
		timeField: r.timeField,
		duration:  octoDuration,
	}, nil
}

type TumbleStream struct {
	source    execution.RecordStream
	timeField string
	duration  octosql.Duration
}

func (s *TumbleStream) Next() (*execution.Record, error) {

	return nil, nil
}

func (s *TumbleStream) Close() error {
	return nil
}
