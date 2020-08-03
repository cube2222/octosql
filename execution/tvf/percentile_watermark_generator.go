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

func (r *PercentileWatermarkGenerator) Document() docs.Documentation {
	return docs.Section(
		"watermark generator: percentile",
		docs.Body(
			docs.Section("Calling", docs.Text("percentile_watermark(source => \\<Source\\>, time_field => \\<Descriptor\\>, events => \\<int\\>, percentile => \\<float\\>, frequency => \\<int\\>)")),
			docs.Section("Description", docs.Body(
				docs.Text("Creating percentile watermark that stores watermark value based on percentile of event_times of recently seen events.\nFields explanation:"),
				docs.List(
					docs.Text("`events` (must be positive) - represents amount of (most recent) events stored"),
					docs.Text("`percentile` (must be positive and less than 100.0) - represents percentile of recently stored events that are BIGGER than watermark value. \n\tEx. if percentile = 35 then 35% of events stored must be bigger than watermark value, so watermark position is at 65th percentile of events stored (in sorted way)"),
					docs.Text("`frequency` (must be positive) - represents amount of events to be seen before initiating next watermark update"),
				),
			)),
			docs.Section("Example", docs.Text("```\nWITH"+
				"\n     with_watermark AS (SELECT * FROM percentile_watermark("+
				"\n                        source=>TABLE(events),"+
				"\n                        time_field=>DESCRIPTOR(time)) e),"+
				"\n                        events=>5,"+
				"\n                        percentile=>30.0,"+
				"\n                        frequency=>2) e),"+
				"\nSELECT e.team, COUNT(*) as goals\nFROM with_watermark e\nGROUP BY e.team\nTRIGGER COUNTING 100, ON WATERMARK"+
				"\n```")),
		),
	)
}

func (w *PercentileWatermarkGenerator) Get(ctx context.Context, variables octosql.Variables, streamID *execution.StreamID) (execution.RecordStream, *execution.ExecutionOutput, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	sourceStreamID, err := execution.GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakePhantom())
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source stream ID")
	}

	source, execOutput, err := w.source.Get(ctx, variables, sourceStreamID)
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
	if percentile.GetType() != octosql.TypeFloat || percentile.AsFloat() < 0.0 || percentile.AsFloat() > 100.0 {
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
		streamID:   streamID,
		source:     source,
		timeField:  w.timeField,
		events:     events.AsInt(),
		percentile: percentile.AsFloat(),
		frequency:  frequency.AsInt(),
	}

	return ws, execution.NewExecutionOutput(ws, execOutput.NextShuffles, execOutput.TasksToRun), nil // watermark generator stream now indicates new watermark source
}

// The way this watermark generator is working is following:
// - events (must be positive) - represents amount of (most recent) events stored
// - percentile (must be positive and less than 100.0) - represents percentile of recently stored events that are BIGGER than watermark value
// 		ex. if percentile = 35 then 35% of events stored must be bigger than watermark value, so watermark position is at
//			65th percentile of events stored (in sorted way)
// - frequency (must be positive) - represents amount of events to be seen before initiating next watermark update
// Generator stores recent events in deque, their counts in map and number of events seen before watermark update in value state
// After <frequency> number of events it updates watermark
type PercentileWatermarkGeneratorStream struct {
	streamID   *execution.StreamID
	source     execution.RecordStream
	timeField  octosql.VariableName
	events     int
	percentile float64
	frequency  int
}

var (
	percentileWatermarkPrefix = []byte("$percentile_watermark$")

	percentileWatermarkEventsPrefix      = []byte("$percentile_watermark_events$")
	percentileWatermarkEventsCountPrefix = []byte("$percentile_watermark_events_count$")
	percentileWatermarkEventsSeenPrefix  = []byte("$percentile_watermark_events_seen$")
)

func (s *PercentileWatermarkGeneratorStream) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	return s.getWatermark(tx.WithPrefix(s.streamID.AsPrefix()))
}

func (s *PercentileWatermarkGeneratorStream) getWatermark(tx storage.StateTransaction) (time.Time, error) {
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

	tx := storage.GetStateTransactionFromContext(ctx).WithPrefix(s.streamID.AsPrefix())

	watermarkStorage := storage.NewValueState(tx.WithPrefix(percentileWatermarkPrefix))
	eventsStorage := storage.NewDeque(tx.WithPrefix(percentileWatermarkEventsPrefix))
	eventsCountStorage := storage.NewMap(tx.WithPrefix(percentileWatermarkEventsCountPrefix))
	eventsSeenStorage := storage.NewValueState(tx.WithPrefix(percentileWatermarkEventsSeenPrefix))

	// Extracting eventsSeen value (remember: this equals to number of events seen before watermark update)
	var eventsSeen octosql.Value
	err = eventsSeenStorage.Get(&eventsSeen)
	if err == storage.ErrNotFound {
		eventsSeen = octosql.MakeInt(0)
	} else if err != nil {
		return nil, errors.Wrap(err, "couldn't get events seen count")
	}
	eventsSeen = octosql.MakeInt(eventsSeen.AsInt() + 1) // Adding +1 as we've just extracted next record from source

	// Adding newest event
	if err = s.addNewestEvent(timeValue, tx); err != nil {
		return nil, errors.Wrap(err, "couldn't add newest event to storage")
	}

	eventsLength, err := eventsStorage.Length()
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get events deque length")
	}

	// In this situation we need to remove oldest event => this doesn't occur only when the oldest event seen is the first ever and length = <events> (update watermark but DON'T remove oldest one)
	if eventsLength > s.events {
		if err := s.removeOldestEvent(tx); err != nil {
			return nil, errors.Wrap(err, "couldn't remove oldest event from storage")
		}
	}

	// Updating watermark; enough events stored in deque AND frequency level reached
	// '>=' for the case when we doesn't remove oldest event (explained above)
	if eventsLength >= s.events && eventsSeen.AsInt() >= s.frequency {

		// Resetting events seen
		eventsSeen = octosql.MakeInt(0)

		// Below declaration equals to (percentile*1000 / 100*1000) = (events - wP) / events
		// Using (events - wP) instead of wP, because percentile of 80% means, that 80% events can be BIGGER than watermark value
		// so watermark position is at 20% of all events
		// Multiplying percentile by 1000 to allow float values like 99.9%
		watermarkPosition := ((100000 - int(s.percentile*1000)) * s.events) / 100000 // represents position (from the left) of event in sorted list that will become new watermark

		// Let's begin iterating through events count map
		eventsAlreadySeen := 0

		var key, value octosql.Value
		eventsIterator := eventsCountStorage.GetIterator()
		for {
			err := eventsIterator.Next(&key, &value)
			if err == storage.ErrEndOfIterator {
				break
			} else if err != nil {
				return nil, errors.Wrap(err, "couldn't get next value from events count map iterator")
			}

			eventsAlreadySeen += value.AsInt()

			if eventsAlreadySeen >= watermarkPosition { // we've passed specified percentile of all events
				break
			}
		}

		err = eventsIterator.Close()
		if err != nil {
			return nil, errors.Wrap(err, "couldn't close events count map iterator")
		}

		// Setting new watermark value
		oldWatermark, err := s.getWatermark(tx)
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

	// Updating events seen
	err = eventsSeenStorage.Set(&eventsSeen)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't update events seen")
	}

	srcRecord = execution.NewRecordFromRecord(srcRecord, execution.WithEventTimeField(s.timeField))

	return srcRecord, nil
}

func (s *PercentileWatermarkGeneratorStream) addNewestEvent(eventTimeValue octosql.Value, tx storage.StateTransaction) error {
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

func (s *PercentileWatermarkGeneratorStream) removeOldestEvent(tx storage.StateTransaction) error {
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

func (s *PercentileWatermarkGeneratorStream) Close(ctx context.Context, storage storage.Storage) error {
	if err := s.source.Close(ctx, storage); err != nil {
		return errors.Wrap(err, "couldn't close underlying stream")
	}

	if err := storage.DropAll(s.streamID.AsPrefix()); err != nil {
		return errors.Wrap(err, "couldn't clear storage with streamID prefix")
	}

	return nil
}
