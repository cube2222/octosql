package tvf

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/storage"
)

func TestPercentileWatermarkGenerator_Get(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2019, 9, 3, 12, 0, 0, 0, time.UTC)

	type fields struct {
		source     execution.Node
		timeField  octosql.VariableName
		events     execution.Expression
		percentile execution.Expression
		frequency  execution.Expression
	}
	type args struct {
		variables octosql.Variables
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    execution.RecordStream
		wantErr bool
	}{
		{
			name: "correct parameters",
			fields: fields{
				source: execution.NewDummyNode([]*execution.Record{
					execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "time"},
						[]interface{}{1, baseTime},
					),
				}),
				timeField:  "time",
				events:     execution.NewVariable(octosql.NewVariableName("events")),
				percentile: execution.NewVariable(octosql.NewVariableName("percentile")),
				frequency:  execution.NewVariable(octosql.NewVariableName("frequency")),
			},
			args: args{
				variables: octosql.NewVariables(map[octosql.VariableName]octosql.Value{
					"events":     octosql.MakeInt(10),
					"percentile": octosql.MakeFloat(0.001),
					"frequency":  octosql.MakeInt(5),
				}),
			},
			wantErr: false,
		},
		{
			name: "wrong events",
			fields: fields{
				source: execution.NewDummyNode([]*execution.Record{
					execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "time"},
						[]interface{}{1, baseTime},
					),
				}),
				timeField:  "time",
				events:     execution.NewVariable(octosql.NewVariableName("events")),
				percentile: execution.NewVariable(octosql.NewVariableName("percentile")),
				frequency:  execution.NewVariable(octosql.NewVariableName("frequency")),
			},
			args: args{
				variables: octosql.NewVariables(map[octosql.VariableName]octosql.Value{
					"events":     octosql.MakeInt(0),
					"percentile": octosql.MakeFloat(40),
					"frequency":  octosql.MakeInt(5),
				}),
			},
			wantErr: true,
		},
		{
			name: "wrong percentile",
			fields: fields{
				source: execution.NewDummyNode([]*execution.Record{
					execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "time"},
						[]interface{}{1, baseTime},
					),
				}),
				timeField:  "time",
				events:     execution.NewVariable(octosql.NewVariableName("events")),
				percentile: execution.NewVariable(octosql.NewVariableName("percentile")),
				frequency:  execution.NewVariable(octosql.NewVariableName("frequency")),
			},
			args: args{
				variables: octosql.NewVariables(map[octosql.VariableName]octosql.Value{
					"events":     octosql.MakeInt(10),
					"percentile": octosql.MakeFloat(100.1),
					"frequency":  octosql.MakeInt(5),
				}),
			},
			wantErr: true,
		},
		{
			name: "wrong frequency",
			fields: fields{
				source: execution.NewDummyNode([]*execution.Record{
					execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "time"},
						[]interface{}{1, baseTime},
					),
				}),
				timeField:  "time",
				events:     execution.NewVariable(octosql.NewVariableName("events")),
				percentile: execution.NewVariable(octosql.NewVariableName("percentile")),
				frequency:  execution.NewVariable(octosql.NewVariableName("frequency")),
			},
			args: args{
				variables: octosql.NewVariables(map[octosql.VariableName]octosql.Value{
					"events":     octosql.MakeInt(10),
					"percentile": octosql.MakeFloat(99.9),
					"frequency":  octosql.MakeInt(-1),
				}),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wg := &PercentileWatermarkGenerator{
				source:     tt.fields.source,
				timeField:  tt.fields.timeField,
				events:     tt.fields.events,
				percentile: tt.fields.percentile,
				frequency:  tt.fields.frequency,
			}

			stateStorage := storage.GetTestStorage(t)

			tx := stateStorage.BeginTransaction()
			ctx := storage.InjectStateTransaction(ctx, tx)

			_, _, err := wg.Get(ctx, tt.args.variables, execution.GetRawStreamID())
			if (err != nil) != tt.wantErr {
				t.Errorf("PercentileWatermarkGenerator.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPercentileWatermarkGenerator_Stream(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2019, 9, 3, 12, 0, 0, 0, time.UTC)

	type fields struct {
		source     execution.Node
		timeField  octosql.VariableName
		events     execution.Expression
		percentile execution.Expression
		frequency  execution.Expression
	}
	type args struct {
		variables octosql.Variables
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    execution.Node
		wantErr bool
	}{
		{
			name: "watermark generator simple",
			fields: fields{
				source: execution.NewDummyNode([]*execution.Record{
					execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "time"},
						[]interface{}{1, baseTime},
						execution.WithEventTimeField(octosql.NewVariableName("time")),
					),
					execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "time"},
						[]interface{}{2, baseTime.Add(time.Second * 10)},
						execution.WithEventTimeField(octosql.NewVariableName("time")),
					),
					execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "time"},
						[]interface{}{3, baseTime.Add(time.Second * 8)},
						execution.WithEventTimeField(octosql.NewVariableName("time")),
					),
					execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "time"},
						[]interface{}{4, baseTime.Add(time.Second * 13)},
						execution.WithEventTimeField(octosql.NewVariableName("time")),
					),
				}),
				timeField:  "time",
				events:     execution.NewVariable(octosql.NewVariableName("events")),
				percentile: execution.NewVariable(octosql.NewVariableName("percentile")),
				frequency:  execution.NewVariable(octosql.NewVariableName("frequency")),
			},
			args: args{
				variables: octosql.NewVariables(map[octosql.VariableName]octosql.Value{
					"events":     octosql.MakeInt(10),
					"percentile": octosql.MakeFloat(40),
					"frequency":  octosql.MakeInt(5),
				}),
			},
			want: execution.NewDummyNode([]*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "time"},
					[]interface{}{1, baseTime},
					execution.WithEventTimeField(octosql.NewVariableName("time")),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "time"},
					[]interface{}{2, baseTime.Add(time.Second * 10)},
					execution.WithEventTimeField(octosql.NewVariableName("time")),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "time"},
					[]interface{}{3, baseTime.Add(time.Second * 8)},
					execution.WithEventTimeField(octosql.NewVariableName("time")),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "time"},
					[]interface{}{4, baseTime.Add(time.Second * 13)},
					execution.WithEventTimeField(octosql.NewVariableName("time")),
				),
			}),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wg := &PercentileWatermarkGenerator{
				source:     tt.fields.source,
				timeField:  tt.fields.timeField,
				events:     tt.fields.events,
				percentile: tt.fields.percentile,
				frequency:  tt.fields.frequency,
			}

			stateStorage := storage.GetTestStorage(t)

			tx := stateStorage.BeginTransaction()
			ctx := storage.InjectStateTransaction(ctx, tx)

			got, _, err := wg.Get(ctx, tt.args.variables, execution.GetRawStreamID())
			if (err != nil) != tt.wantErr {
				t.Errorf("PercentileWatermarkGenerator.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			wanted, _, err := tt.want.Get(ctx, tt.args.variables, execution.GetRawStreamID())
			if err != nil {
				t.Errorf("PercentileWatermarkGenerator.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			err = execution.AreStreamsEqual(ctx, got, wanted)
			if err != nil {
				t.Errorf("PercentileWatermarkGenerator.Get() AreStreamsEqual error = %v", err)
			}

			if err := got.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close percentile watermark generator stream: %v", err)
				return
			}
			if err := wanted.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close wanted in_memory stream: %v", err)
				return
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPercentileWatermarkGeneratorStream_GetWatermark(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2019, 9, 3, 12, 0, 0, 0, time.UTC)

	variables := octosql.NewVariables(map[octosql.VariableName]octosql.Value{
		"events":     octosql.MakeInt(5),
		"percentile": octosql.MakeFloat(59.9), // 59.9% events bigger than watermark => watermark at 40.1% => watermarkPosition = (40.1*1000/100*1000) * 5 = 2.005 => = 2 (cutting down decimal part)
		"frequency":  octosql.MakeInt(2),
	})

	// event times seen : 1, 0, 5, 3, 2, 2, 4, 1, 6, 8, 7, 9, 7, 8, 42
	source := execution.NewDummyNode([]*execution.Record{
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"id", "time"},
			[]interface{}{1, baseTime.Add(time.Second * 1)},
		),
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"id", "time"},
			[]interface{}{2, baseTime},
		),
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"id", "time"},
			[]interface{}{3, baseTime.Add(time.Second * 5)},
		),
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"id", "time"},
			[]interface{}{4, baseTime.Add(time.Second * 3)},
		),
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"id", "time"},
			[]interface{}{3, baseTime.Add(time.Second * 2)},
		),
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"id", "time"},
			[]interface{}{4, baseTime.Add(time.Second * 2)},
		),
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"id", "time"},
			[]interface{}{3, baseTime.Add(time.Second * 4)},
		),
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"id", "time"},
			[]interface{}{4, baseTime.Add(time.Second * 1)},
		),
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"id", "time"},
			[]interface{}{3, baseTime.Add(time.Second * 6)},
		),
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"id", "time"},
			[]interface{}{4, baseTime.Add(time.Second * 8)},
		),
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"id", "time"},
			[]interface{}{3, baseTime.Add(time.Second * 7)},
		),
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"id", "time"},
			[]interface{}{4, baseTime.Add(time.Second * 9)},
		),
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"id", "time"},
			[]interface{}{3, baseTime.Add(time.Second * 7)},
		),
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"id", "time"},
			[]interface{}{4, baseTime.Add(time.Second * 8)},
		),
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"id", "time"},
			[]interface{}{4, baseTime.Add(time.Second * 42)},
		),
	})
	timeField := octosql.NewVariableName("time")
	events := execution.NewVariable(octosql.NewVariableName("events"))
	percentile := execution.NewVariable(octosql.NewVariableName("percentile"))
	frequency := execution.NewVariable(octosql.NewVariableName("frequency"))

	wg := &PercentileWatermarkGenerator{
		source:     source,
		timeField:  timeField,
		events:     events,
		percentile: percentile,
		frequency:  frequency,
	}

	stateStorage := storage.GetTestStorage(t)
	streamID := execution.GetRawStreamID()

	tx := stateStorage.BeginTransaction()
	ctx = storage.InjectStateTransaction(ctx, tx)

	src, execOutput, err := wg.Get(ctx, variables, streamID)
	if err != nil {
		t.Errorf("PercentileWatermarkGenerator.Get() error = %v", err)
		return
	}

	assert.Equal(t, src, execOutput.WatermarkSource)

	ws := execOutput.WatermarkSource

	// event times seen : 1, 0, 5, 3, 2, 2, 4, 1, 6, 8, 7, 9, 7, 8, 42

	// 1) Not enough events seen => in that case return time.Time{}
	ExpectWatermarkValue(t, ctx, ws, tx, time.Time{})

	NextRecord(t, ctx, src) // event: 1 ; deque: 1
	ExpectWatermarkValue(t, ctx, ws, tx, time.Time{})

	NextRecord(t, ctx, src) // event: 0 ; deque: 1 0
	ExpectWatermarkValue(t, ctx, ws, tx, time.Time{})

	NextRecord(t, ctx, src) // event: 5 ; deque: 1 0 5
	ExpectWatermarkValue(t, ctx, ws, tx, time.Time{})

	NextRecord(t, ctx, src) // event: 3 ; deque: 1 0 5 3
	ExpectWatermarkValue(t, ctx, ws, tx, time.Time{})

	// 2) Deque length reaches <events> in that case we begin updating watermark cycle (happening every <frequency> events seen)
	// As said above, watermark position is 2
	// This is THE ONLY situation when we don't want to remove oldest element yet we want to update watermark
	NextRecord(t, ctx, src)                                           // event: 2 ; deque: 1 0 5 3 2 ; sorted: 0 1 2 3 5
	ExpectWatermarkValue(t, ctx, ws, tx, baseTime.Add(time.Second*1)) // update: eventsSeen = *__5__*, frequency = 2

	NextRecord(t, ctx, src)                                           // event: 2 ; deque: 0 5 3 2 2 ; sorted: 0 2 2 3 5
	ExpectWatermarkValue(t, ctx, ws, tx, baseTime.Add(time.Second*1)) // no update: eventsSeen = 1, frequency = 2

	NextRecord(t, ctx, src)                                           // event: 4 ; deque: 5 3 2 2 4 ; sorted: 2 2 3 4 5
	ExpectWatermarkValue(t, ctx, ws, tx, baseTime.Add(time.Second*2)) // update: eventsSeen = 2, frequency = 2

	NextRecord(t, ctx, src)                                           // event: 1 ; deque: 3 2 2 4 1 ; sorted: 1 2 2 3 3
	ExpectWatermarkValue(t, ctx, ws, tx, baseTime.Add(time.Second*2)) // no update: eventsSeen = 1, frequency = 2

	NextRecord(t, ctx, src)                                           // event: 6 ; deque: 2 2 4 1 6 ; sorted: 1 2 2 4 6
	ExpectWatermarkValue(t, ctx, ws, tx, baseTime.Add(time.Second*2)) // update: eventsSeen = 2, frequency = 2

	NextRecord(t, ctx, src)                                           // event: 8 ; deque: 2 4 1 6 8 ; sorted: 1 2 4 6 8
	ExpectWatermarkValue(t, ctx, ws, tx, baseTime.Add(time.Second*2)) // no update

	NextRecord(t, ctx, src)                                           // event: 7 ; deque: 4 1 6 8 7 ; sorted: 1 4 6 7 8
	ExpectWatermarkValue(t, ctx, ws, tx, baseTime.Add(time.Second*4)) // update

	NextRecord(t, ctx, src)                                           // event: 9 ; deque: 1 6 8 7 9 ; sorted: 1 6 7 8 9
	ExpectWatermarkValue(t, ctx, ws, tx, baseTime.Add(time.Second*4)) // no update

	NextRecord(t, ctx, src)                                           // event: 7 ; deque: 6 8 7 9 7 ; sorted: 6 7 7 8 9
	ExpectWatermarkValue(t, ctx, ws, tx, baseTime.Add(time.Second*7)) // update

	NextRecord(t, ctx, src)                                           // event: 8 ; deque: 8 7 9 7 8 ; sorted: 7 7 8 8 9
	ExpectWatermarkValue(t, ctx, ws, tx, baseTime.Add(time.Second*7)) // no update

	NextRecord(t, ctx, src)                                           // event: 42 ; deque: 7 9 7 8 42; sorted: 7 7 8 9 42
	ExpectWatermarkValue(t, ctx, ws, tx, baseTime.Add(time.Second*7)) // update

	if err := src.Close(ctx, stateStorage); err != nil {
		t.Errorf("Couldn't close percentile watermark generator stream: %v", err)
		return
	}
}
