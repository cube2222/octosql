package tvf

import (
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/storage"

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
			docs.Section("Calling", docs.Text("range(range_start => \\<int\\>, range_end => \\<int\\>)")),
			docs.Section("Description", docs.Text("Returns a list of numbers from range_start inclusive to range_end exclusive. The field name of the number will be i.")),
			docs.Section("Example", docs.Text("`SELECT * FROM range(range_start => 1, range_end => 5) r`")),
		),
	)
}

var currentIndexPrefix = []byte("$range_current_index$")

func (r *Range) Get(ctx context.Context, variables octosql.Variables, streamID *execution.StreamID) (execution.RecordStream, *execution.ExecutionOutput, error) {
	start, err := r.start.ExpressionValue(ctx, variables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get range start point")
	}
	if start.GetType() != octosql.TypeInt {
		return nil, nil, errors.Errorf("invalid range start point: %v", start)
	}

	end, err := r.end.ExpressionValue(ctx, variables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get range end point")
	}
	if end.GetType() != octosql.TypeInt {
		return nil, nil, errors.Errorf("invalid range start point: %v", end)
	}

	currentIndexState := storage.NewValueState(storage.GetStateTransactionFromContext(ctx).WithPrefix(streamID.AsPrefix()).WithPrefix(currentIndexPrefix))
	if err := currentIndexState.Set(&start); err != nil {
		return nil, nil, errors.Wrap(err, "couldn't set current index in range table valued function")
	}

	return &RangeStream{
		streamID:     streamID,
		endExclusive: end.AsInt(),
	}, execution.NewExecutionOutput(execution.NewZeroWatermarkGenerator(), map[string]execution.ShuffleData{}, nil), nil
}

type RangeStream struct {
	streamID     *execution.StreamID
	endExclusive int
}

func (s *RangeStream) Next(ctx context.Context) (*execution.Record, error) {
	currentIndexState := storage.NewValueState(storage.GetStateTransactionFromContext(ctx).WithPrefix(s.streamID.AsPrefix()).WithPrefix(currentIndexPrefix))

	var currentIndex octosql.Value
	if err := currentIndexState.Get(&currentIndex); err != nil {
		return nil, errors.Wrap(err, "couldn't get current index in range table valued function")
	}

	if currentIndex.AsInt() >= s.endExclusive {
		return nil, execution.ErrEndOfStream
	}

	out := execution.NewRecordFromSlice([]octosql.VariableName{"i"}, []octosql.Value{currentIndex})

	currentIndex = octosql.MakeInt(currentIndex.AsInt() + 1)
	if err := currentIndexState.Set(&currentIndex); err != nil {
		return nil, errors.Wrap(err, "couldn't save new current index in range table valued function")
	}

	return out, nil
}

func (s *RangeStream) Close(ctx context.Context, storage storage.Storage) error {
	if err := storage.DropAll(s.streamID.AsPrefix()); err != nil {
		return errors.Wrap(err, "couldn't clear storage with streamID prefix")
	}

	return nil
}
