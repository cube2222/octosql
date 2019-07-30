package tvf

import (
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution"
	"github.com/pkg/errors"
)

type Range struct {
	start, end execution.Expression
}

func NewRange(start, end execution.Expression) *Range {
	return &Range{
		start: start,
		end:   end,
	}
}

func (r *Range) Document() docs.Documentation {
	return docs.Section(
		"range",
		docs.Body(
			docs.Section("Calling", docs.List(docs.Text("range(range_start => \\<int\\>, range_end => \\<int\\>)"))),
			docs.Section("Description", docs.Text("Returns a list of numbers from range_start inclusive to range_end exclusive. The field name of the number will be i.")),
		),
	)
}

func (r *Range) Get(variables octosql.Variables) (execution.RecordStream, error) {
	start, err := r.start.ExpressionValue(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get range start point")
	}
	octoStart, ok := start.(octosql.Int)
	if !ok {
		return nil, errors.Errorf("invalid range start point: %v", start)
	}

	end, err := r.end.ExpressionValue(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get range end point")
	}
	octoEnd, ok := end.(octosql.Int)
	if !ok {
		return nil, errors.Errorf("invalid range start point: %v", end)
	}

	return &RangeStream{
		current:      octoStart,
		endExclusive: octoEnd,
	}, nil
}

type RangeStream struct {
	current, endExclusive octosql.Int
}

func (s *RangeStream) Next() (*execution.Record, error) {
	if s.current >= s.endExclusive {
		return nil, execution.ErrEndOfStream
	}

	out := execution.NewRecordFromSlice([]octosql.VariableName{"i"}, []octosql.Value{s.current})
	s.current++

	return out, nil
}

func (s *RangeStream) Close() error {
	s.current = s.endExclusive
	return nil
}
