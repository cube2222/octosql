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
	frequency  execution.Expression
}

func NewPercentileWatermarkGenerator(source execution.Node, timeField octosql.VariableName, events, percentile, frequency execution.Expression) *PercentileWatermarkGenerator {
	return &PercentileWatermarkGenerator{
		source:     source,
		timeField:  timeField,
		events:     events,
		percentile: percentile,
		frequency:  frequency,
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
	if events.GetType() != octosql.TypeInt || events.AsInt() < 1 {
		return nil, nil, errors.Errorf("invalid watermark events: %v", events)
	}

	percentile, err := w.percentile.ExpressionValue(ctx, variables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get watermark percentile")
	}
	if percentile.GetType() != octosql.TypeInt || percentile.AsInt() < 1 || percentile.AsInt() > 99 {
		return nil, nil, errors.Errorf("invalid watermark percentile: %v", percentile)
	}

	frequency, err := w.frequency.ExpressionValue(ctx, variables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get watermark frequency")
	}
	if frequency.GetType() != octosql.TypeInt || frequency.AsInt() < 1 {
		return nil, nil, errors.Errorf("invalid watermark frequency: %v", frequency)
	}

	ws := &PercentileWatermarkGeneratorStream{
		source:     source,
		timeField:  w.timeField,
		events:     events.AsInt(),
		percentile: percentile.AsInt(),
		frequency:  frequency.AsInt(),
	}

	return ws, execution.NewExecOutput(ws), nil // watermark generator stream now indicates new watermark source
}

// The way this watermark generator is working is following:
// - events (must be positive) - represents amount of (most recent) events stored
// - percentile (must be between 1 and 99) - represents percentile of recently stored events that are BIGGER than watermark value
// 		ex. if percentile = 35 then 35% of events stored must be bigger than watermark value, so watermark position is at
//			65th percentile of events stored (in sorted way)
// - frequency (must be positive) - represents amount of events to be seen to initiate updating watermark process
// Generator stores recent events in deque, their counts in map and number of events seen before watermark update in value state
// After <frequency> number of events it updates watermark
type PercentileWatermarkGeneratorStream struct {
	source     execution.RecordStream
	timeField  octosql.VariableName
	events     int
	percentile int
	frequency  int
}

var (
	percentileWatermarkPrefix = []byte("$percentile_watermark$")

	percentileWatermarkEventsPrefix      = []byte("$percentile_watermark_events$")
	percentileWatermarkEventsCountPrefix = []byte("$percentile_watermark_events_count$")
	percentileWatermarkEventsSeenPrefix  = []byte("$percentile_watermark_events_seen$")
)

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
	eventsCountStorage := storage.NewMap(tx.WithPrefix(percentileWatermarkEventsCountPrefix))
	eventsSeenStorage := storage.NewValueState(tx.WithPrefix(percentileWatermarkEventsSeenPrefix))

	// Adding newest event
	err = addNewestEvent(timeValue, tx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't add newest event to storage")
	}

	eventsLength, err := eventsStorage.Length()
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get events deque length")
	}

	var eventsSeen octosql.Value
	err = eventsSeenStorage.Get(&eventsSeen)
	if err == storage.ErrNotFound {
		eventsSeen = octosql.MakeInt(0)
	} else if err != nil {
		return nil, errors.Wrap(err, "couldn't get events seen count")
	}
	eventsSeen = octosql.MakeInt(eventsSeen.AsInt() + 1) // Adding +1 as we've just extracted next record from source

	// There are enough events seen to remove oldest event
	if eventsLength >= s.events {

		// Removing oldest event (no matter if we update watermark or not)
		err := removeOldestEvent(tx)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't remove oldest event from storage")
		}

		// Updating watermark
		// '>=' because if <events> is bigger than <frequency>, then we want to update watermark when deque length reaches <events>
		if eventsSeen.AsInt() >= s.frequency {

			// Clearing events seen
			eventsSeen = octosql.MakeInt(0)

			// Below declaration equals to (percentile / 100) = (events - wP) / events
			// Using (events - wP) instead of wP, because percentile of 80% means, that 80% events can be BIGGER than watermark value
			// so watermark position is at 20% of all events
			watermarkPosition := ((100 - s.percentile) * s.events) / 100 // represents position (from the left) of event in sorted list that will become new watermark

			// Let's begin iterating through events count map
			eventsAlreadySeen := 0

			var key, value octosql.Value
			eventsIterator := eventsCountStorage.GetIterator()
			for {
				err := eventsIterator.Next(&key, &value)
				if err == storage.ErrEndOfIterator {
					break
				} else if err != nil {
					return nil, errors.Wrap(err, "couldn't get next value from iterator")
				}

				eventsAlreadySeen += value.AsInt()

				if eventsAlreadySeen >= watermarkPosition { // we've passed specified percentile of all events
					break
				}
			}

			// Setting new watermark value
			oldWatermark, err := s.GetWatermark(ctx, tx)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get old watermark value from storage")
			}

			newWatermark := octosql.MakeTime(key.AsTime())

			if newWatermark.AsTime().After(oldWatermark) { // watermarks can't decrease
				err = watermarkStorage.Set(&newWatermark)
				if err != nil {
					return nil, errors.Wrap(err, "couldn't set new watermark value in storage")
				}
			}
		}
	}

	// Updating events seen
	err = eventsSeenStorage.Set(&eventsSeen)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't update events seen")
	}

	return srcRecord, nil
}

func addNewestEvent(eventTimeValue octosql.Value, tx storage.StateTransaction) error {
	eventsStorage := storage.NewDeque(tx.WithPrefix(percentileWatermarkEventsPrefix))
	eventsCountStorage := storage.NewMap(tx.WithPrefix(percentileWatermarkEventsCountPrefix))

	// Adding to events deque
	err := eventsStorage.PushBack(&eventTimeValue)
	if err != nil {
		return errors.Wrap(err, "couldn't push back new event to events deque")
	}

	var oldCount octosql.Value
	var newCount octosql.Value

	// Adding to events count map
	err = eventsCountStorage.Get(&eventTimeValue, &oldCount)
	if err != nil {
		if err == storage.ErrNotFound {
			newCount = octosql.MakeInt(1)
		} else {
			return errors.Wrap(err, "couldn't get newest event count from events count map")
		}
	} else {
		newCount = octosql.MakeInt(oldCount.AsInt() + 1)
	}

	err = eventsCountStorage.Set(&eventTimeValue, &newCount)
	if err != nil {
		return errors.Wrap(err, "couldn't set newest event count to events count map")
	}

	return nil
}

func removeOldestEvent(tx storage.StateTransaction) error {
	eventsStorage := storage.NewDeque(tx.WithPrefix(percentileWatermarkEventsPrefix))
	eventsCountStorage := storage.NewMap(tx.WithPrefix(percentileWatermarkEventsCountPrefix))

	var oldestEventTimeValue octosql.Value

	// Removing from events deque
	err := eventsStorage.PopFront(&oldestEventTimeValue)
	if err != nil {
		return errors.Wrap(err, "couldn't pop front oldest event from events deque")
	}

	var oldestCount octosql.Value

	// Removing from events count map
	err = eventsCountStorage.Get(&oldestEventTimeValue, &oldestCount)
	if err != nil {
		return errors.Wrap(err, "couldn't get oldest event count from events count map")
	}
	if oldestCount.AsInt() == 1 {
		err = eventsCountStorage.Delete(&oldestEventTimeValue)
		if err != nil {
			return errors.Wrap(err, "couldn't delete oldest event from events count map")
		}
	} else {
		newCount := octosql.MakeInt(oldestCount.AsInt() - 1)
		err = eventsCountStorage.Set(&oldestEventTimeValue, &newCount)
		if err != nil {
			return errors.Wrap(err, "couldn't set oldest event count to events count map")
		}
	}

	return nil
}

func (s *PercentileWatermarkGeneratorStream) Close() error {
	return s.source.Close()
}
