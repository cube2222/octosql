package tvf

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/storage"
)

type MaximumDifferenceWatermarkGenerator struct {
	source    execution.Node
	timeField octosql.VariableName
	offset    execution.Expression
}

func NewMaximumDifferenceWatermarkGenerator(source execution.Node, timeField octosql.VariableName, offset execution.Expression) *MaximumDifferenceWatermarkGenerator {
	return &MaximumDifferenceWatermarkGenerator{
		source:    source,
		timeField: timeField,
		offset:    offset,
	}
}

func (r *MaximumDifferenceWatermarkGenerator) Document() docs.Documentation {
	return docs.Section(
		"watermark generator: maximal difference",
		docs.Body(
			docs.Section("Calling", docs.Text("max_diff_watermark(source => \\<Source\\>, time_field => \\<Descriptor\\>, offset => \\<interval\\>)")),
			docs.Section("Description", docs.Text("Creating standard watermark that stores watermark value of `<maximal record event time> - given offset.`")),
			docs.Section("Example", docs.Text("```\nWITH"+
				"\n     with_watermark AS (SELECT * FROM max_diff_watermark("+
				"\n                        source=>TABLE(events),"+
				"\n                        time_field=>DESCRIPTOR(time)),"+
				"\n                        offset=>INTERVAL 5 SECONDS) e),"+
				"\nSELECT e.team, COUNT(*) as goals\nFROM with_watermark e\nGROUP BY e.team\nTRIGGER COUNTING 100, ON WATERMARK"+
				"\n```")),
		),
	)
}

func (w *MaximumDifferenceWatermarkGenerator) Get(ctx context.Context, variables octosql.Variables, streamID *execution.StreamID) (execution.RecordStream, *execution.ExecutionOutput, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	sourceStreamID, err := execution.GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakePhantom())
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source stream ID")
	}

	source, execOutput, err := w.source.Get(ctx, variables, sourceStreamID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source")
	}

	offset, err := w.offset.ExpressionValue(ctx, variables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get watermark offset")
	}
	if offset.GetType() != octosql.TypeDuration {
		return nil, nil, errors.Errorf("invalid watermark offset type: %v", offset.GetType())
	}

	ws := &WatermarkGeneratorStream{
		streamID:  streamID,
		source:    source,
		timeField: w.timeField,
		offset:    offset.AsDuration(),
	}

	return ws, execution.NewExecutionOutput(ws, execOutput.NextShuffles, execOutput.TasksToRun), nil // watermark generator stream now indicates new watermark source
}

type WatermarkGeneratorStream struct {
	streamID  *execution.StreamID
	source    execution.RecordStream
	timeField octosql.VariableName
	offset    time.Duration
}

var watermarkPrefix = []byte("$watermark$")

func (s *WatermarkGeneratorStream) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	return s.getWatermark(tx.WithPrefix(s.streamID.AsPrefix()))
}

func (s *WatermarkGeneratorStream) getWatermark(tx storage.StateTransaction) (time.Time, error) {
	watermarkStorage := storage.NewValueState(tx.WithPrefix(watermarkPrefix))

	var currentWatermark octosql.Value
	err := watermarkStorage.Get(&currentWatermark)
	if err == storage.ErrNotFound {
		currentWatermark = octosql.MakeTime(time.Time{})
	} else if err != nil {
		return time.Time{}, errors.Wrap(err, "couldn't get current watermark from storage")
	}

	return currentWatermark.AsTime(), nil
}

func (s *WatermarkGeneratorStream) Next(ctx context.Context) (*execution.Record, error) {
	srcRecord, err := s.source.Next(ctx)
	if err != nil {
		if err == execution.ErrEndOfStream {
			return nil, execution.ErrEndOfStream
		}
		return nil, errors.Wrap(err, "couldn't get source record")
	}

	timeValue := srcRecord.Value(s.timeField)
	if timeValue.GetType() != octosql.TypeTime {
		return nil, fmt.Errorf("couldn't get time field '%v' as time, got: %v", s.timeField.String(), srcRecord.Value(s.timeField))
	}

	// watermark value stored equals to (max_record_time - offset) that's why we multiply offset by -1
	timeValueWithOffset := timeValue.AsTime().Add(-1 * s.offset)

	tx := storage.GetStateTransactionFromContext(ctx).WithPrefix(s.streamID.AsPrefix())

	currentWatermark, err := s.getWatermark(tx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get current watermark value")
	}

	if timeValueWithOffset.After(currentWatermark) { // time in current record is bigger than current watermark - update it
		watermarkStorage := storage.NewValueState(tx.WithPrefix(watermarkPrefix))

		newWatermark := octosql.MakeTime(timeValueWithOffset)
		err := watermarkStorage.Set(&newWatermark)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't set new watermark value in storage")
		}
	}

	srcRecord = execution.NewRecordFromRecord(srcRecord, execution.WithEventTimeField(s.timeField))

	return srcRecord, nil
}

func (s *WatermarkGeneratorStream) Close(ctx context.Context, storage storage.Storage) error {
	if err := s.source.Close(ctx, storage); err != nil {
		return errors.Wrap(err, "couldn't close underlying stream")
	}

	if err := storage.DropAll(s.streamID.AsPrefix()); err != nil {
		return errors.Wrap(err, "couldn't clear storage with streamID prefix")
	}

	return nil
}
