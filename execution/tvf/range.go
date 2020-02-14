package tvf

import (
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution"

	"context"

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

func (r *Range) Get(ctx context.Context, variables octosql.Variables, streamID *execution.StreamID) (execution.RecordStream, error) {
	start, err := r.start.ExpressionValue(ctx, variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get range start point")
	}
	if start.GetType() != octosql.TypeInt {
		return nil, errors.Errorf("invalid range start point: %v", start)
	}

	end, err := r.end.ExpressionValue(ctx, variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get range end point")
	}
	if end.GetType() != octosql.TypeInt {
		return nil, errors.Errorf("invalid range start point: %v", end)
	}

	return &RangeStream{
		current:      start.AsInt(),
		endExclusive: end.AsInt(),
	}, nil
}

type RangeStream struct {
	current, endExclusive int
}

func (s *RangeStream) Next(ctx context.Context) (*execution.Record, error) {
	if s.current >= s.endExclusive {
		return nil, execution.ErrEndOfStream
	}

	out := execution.NewRecordFromSlice([]octosql.VariableName{"i"}, []octosql.Value{octosql.MakeInt(s.current)})
	s.current++

	return out, nil
}

func (s *RangeStream) Close() error {
	s.current = s.endExclusive
	return nil
}
