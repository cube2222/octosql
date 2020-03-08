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

type WatermarkGenerator struct {
	source    execution.Node
	timeField octosql.VariableName
	offset    execution.Expression
}

func NewWatermarkGenerator(source execution.Node, timeField octosql.VariableName, offset execution.Expression) *WatermarkGenerator {
	return &WatermarkGenerator{
		source:    source,
		timeField: timeField,
		offset:    offset,
	}
}

func (w *WatermarkGenerator) Document() docs.Documentation {
	panic("implement me")
}

func (w *WatermarkGenerator) Get(ctx context.Context, variables octosql.Variables, streamID *execution.StreamID) (execution.RecordStream, *execution.ExecutionOutput, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	sourceStreamID, err := execution.GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakePhantom())
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source stream ID")
	}

	source, _, err := w.source.Get(ctx, variables, sourceStreamID) // we don't need execOutput here since we become the new watermark source
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
		source:    source,
		timeField: w.timeField,
		offset:    offset.AsDuration(),
	}

	return ws, execution.NewExecutionOutput(ws), nil // watermark generator stream now indicates new watermark source
}

type WatermarkGeneratorStream struct {
	source    execution.RecordStream
	timeField octosql.VariableName
	offset    time.Duration
}

var watermarkPrefix = []byte("$watermark$")

func (s *WatermarkGeneratorStream) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
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

	tx := storage.GetStateTransactionFromContext(ctx)
	currentWatermark, err := s.GetWatermark(ctx, tx)
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

	return srcRecord, nil
}

func (s *WatermarkGeneratorStream) Close() error {
	return s.source.Close()
}
