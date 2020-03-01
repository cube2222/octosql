package tvf

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/streaming/storage"
)

type PercentileWatermarkGenerator struct {
	source     execution.Node
	timeField  octosql.VariableName
	events     execution.Expression
	percentile execution.Expression
}

func NewPercentileWatermarkGenerator(source execution.Node, timeField octosql.VariableName, events, percentile execution.Expression) *PercentileWatermarkGenerator {
	return &PercentileWatermarkGenerator{
		source:     source,
		timeField:  timeField,
		events:     events,
		percentile: percentile,
	}
}

func (w *PercentileWatermarkGenerator) Get(ctx context.Context, variables octosql.Variables, streamID *execution.StreamID) (execution.RecordStream, *execution.ExecOutput, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	sourceStreamID, err := execution.GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakePhantom())
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source stream ID")
	}

	source, _, err := w.source.Get(ctx, variables, sourceStreamID) // we don't need execOutput here since we become new watermark source
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source")
	}

	events, err := w.events.ExpressionValue(ctx, variables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get watermark events")
	}
	if events.GetType() != octosql.TypeDuration || events.AsInt() < 1 {
		return nil, nil, errors.Errorf("invalid watermark events: %v", events)
	}

	percentile, err := w.percentile.ExpressionValue(ctx, variables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get watermark percentile")
	}
	if percentile.GetType() != octosql.TypeDuration || percentile.AsInt() < 1 || percentile.AsInt() > 99 {
		return nil, nil, errors.Errorf("invalid watermark percentile: %v", percentile)
	}

	ws := &PercentileWatermarkGeneratorStream{
		source:     source,
		timeField:  w.timeField,
		events:     events.AsInt(),
		percentile: percentile.AsInt(),
		eventsSeen: 0,
	}

	return ws, execution.NewExecOutput(ws), nil // watermark generator stream now indicates new watermark source
}

type PercentileWatermarkGeneratorStream struct {
	source     execution.RecordStream
	timeField  octosql.VariableName
	events     int
	percentile int
	eventsSeen int
}

var percentileWatermarkPrefix = []byte("$percentile_watermark$")
var percentileWatermarkEventsPrefix = []byte("$percentile_watermark_events$")

func (s *PercentileWatermarkGeneratorStream) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	watermarkStorage := storage.NewValueState(tx.WithPrefix(percentileWatermarkPrefix))

	var currentWatermark octosql.Value
	err := watermarkStorage.Get(&currentWatermark)
	if err == storage.ErrNotFound {
		currentWatermark = octosql.MakeTime(time.Time{})
	} else if err != nil {
		return time.Time{}, errors.Wrap(err, "couldn't get current watermark from storage")
	}

	return currentWatermark.AsTime(), nil
}

func (s *PercentileWatermarkGeneratorStream) Next(ctx context.Context) (*execution.Record, error) {
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

	tx := storage.GetStateTransactionFromContext(ctx)
	watermarkStorage := storage.NewValueState(tx.WithPrefix(percentileWatermarkPrefix))
	eventsStorage := storage.NewDeque(tx.WithPrefix(percentileWatermarkEventsPrefix))

	var watermarkPlace int       // represents position of event in sorted list that will become new watermark
	if s.eventsSeen < s.events { // no need to pop first element from deque
		s.eventsSeen++

		watermarkPlace = s.percentile * s.eventsSeen / 100
	} else {
		watermarkPlace = s.percentile * s.events / 100

		var firstElem octosql.Value
		err = eventsStorage.PopFront(&firstElem)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't pop front oldest event from events deque")
		}
	}

	err = eventsStorage.PushBack(&timeValue)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't push back new event to events deque")
	}

	// TODO - well, do something clever to save new watermark value ...
	// ...

	newWatermark := octosql.MakeTime(time.Time{})
	err = watermarkStorage.Set(&newWatermark)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't set new watermark value in storage")
	}

	return srcRecord, nil
}

func (s *PercentileWatermarkGeneratorStream) Close() error {
	return s.source.Close()
}
