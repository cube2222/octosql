package streaming

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v2"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/streaming/aggregate"
	"github.com/cube2222/octosql/streaming/storage"
	"github.com/cube2222/octosql/streaming/trigger"
)

func Time(t time.Time) *time.Time {
	return &t
}

func TestStream(t *testing.T) {
	opts := badger.DefaultOptions("test")
	opts.InMemory = true
	opts.Dir = ""
	opts.ValueDir = ""
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		db.Close()
		os.RemoveAll("test")
	}()
	storage := storage.NewBadgerStorage(db)

	now := time.Date(2020, 1, 11, 19, 0, 0, 0, time.Local)
	source := &WatermarkRecordStream{
		stream: []WatermarkRecordStreamEntry{
			{
				watermark: Time(now.Add(time.Second * 3)),
			},
			/*{
				record: execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "color", "window_end"},
					[]interface{}{"Kuba", "red", now.Add(time.Second * 5)},
					execution.WithID(execution.NewID("123")),
					execution.WithEventTimeField("window_end"),
				),
			},*/
			{
				record: execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "color", "window_end"},
					[]interface{}{"Janek", "blue", now.Add(time.Second * 5)},
					execution.WithID(execution.NewID("124")),
					execution.WithEventTimeField("window_end"),
				),
			},
			{
				record: execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "color", "window_end"},
					[]interface{}{"Janek", "blue", now.Add(time.Second * 5)},
					execution.WithID(execution.NewID("124")),
					execution.WithEventTimeField("window_end"),
				),
			},
			{
				record: execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "color", "window_end"},
					[]interface{}{"Janek", "blue", now.Add(time.Second * 10)},
					execution.WithID(execution.NewID("124")),
					execution.WithEventTimeField("window_end"),
				),
			},
			{
				record: execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "color", "window_end"},
					[]interface{}{"Janek", "blue", now.Add(time.Second * 5)},
					execution.WithID(execution.NewID("124")),
					execution.WithEventTimeField("window_end"),
				),
			},
			{
				watermark: Time(now.Add(time.Second * 7)),
			},
			{
				record: execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "color", "window_end"},
					[]interface{}{"Janek", "blue", now.Add(time.Second * 10)},
					execution.WithID(execution.NewID("124")),
					execution.WithEventTimeField("window_end"),
				),
			},
			{
				record: execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "color", "window_end"},
					[]interface{}{"Janek", "blue", now.Add(time.Second * 5)},
					execution.WithID(execution.NewID("124")),
					execution.WithEventTimeField("window_end"),
				),
			},
			{
				record: execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "color", "window_end"},
					[]interface{}{"Janek", "blue", now.Add(time.Second * 10)},
					execution.WithID(execution.NewID("124")),
					execution.WithEventTimeField("window_end"),
				),
			},
			/*{
				record: execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"name", "color", "window_end"},
					[]interface{}{"Wojtek", "greeeen", now.Add(time.Second * 15)},
					execution.WithID(execution.NewID("121")),
					execution.WithEventTimeField("window_end"),
				),
			},*/
		},
	}

	groupBy := &GroupBy{
		prefixes:         [][]byte{nil, []byte("$agg1$")},
		inputFields:      []octosql.VariableName{"window_end", "*star*"},
		eventTimeField:   "window_end",
		aggregates:       []Aggregate{&aggregate.First{}, &aggregate.Count{}},
		outputFieldNames: []octosql.VariableName{"window_end", "*star*_count"},
	}
	processFunc := &ProcessByKey{
		eventTimeField:  "window_end",
		output:          make(chan outputEntry, 1024),
		trigger:         trigger.NewMultiTrigger(trigger.NewWatermarkTrigger(), trigger.NewCountingTrigger(1)),
		keyExpression:   []execution.Expression{execution.NewVariable("window_end"), execution.NewVariable("color")},
		processFunction: groupBy,
		variables:       octosql.NoVariables(),
	}
	groupByPullEngine := NewPullEngine(processFunc, storage, source, source)
	groupByPullEngine.Run(context.Background())

	engine := NewPullEngine(&RecordStorePrint{}, storage, groupByPullEngine, groupByPullEngine)
	engine.Run(context.Background())
}
