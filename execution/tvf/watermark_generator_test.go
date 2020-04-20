package tvf

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestWatermarkGenerator_Get(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2019, 9, 3, 12, 0, 0, 0, time.UTC)

	type fields struct {
		source    execution.Node
		timeField octosql.VariableName
		offset    execution.Expression
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
					),
					execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "time"},
						[]interface{}{2, baseTime.Add(time.Second * 10)},
					),
					execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "time"},
						[]interface{}{3, baseTime.Add(time.Second * 8)},
					),
					execution.NewRecordFromSliceWithNormalize(
						[]octosql.VariableName{"id", "time"},
						[]interface{}{4, baseTime.Add(time.Second * 13)},
					),
				}),
				timeField: "time",
				offset:    execution.NewVariable(octosql.NewVariableName("offset")),
			},
			args: args{
				variables: octosql.NewVariables(map[octosql.VariableName]octosql.Value{
					"offset": octosql.MakeDuration(time.Second * 5),
				}),
			},
			want: execution.NewDummyNode([]*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "time"},
					[]interface{}{1, baseTime},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "time"},
					[]interface{}{2, baseTime.Add(time.Second * 10)},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "time"},
					[]interface{}{3, baseTime.Add(time.Second * 8)},
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{"id", "time"},
					[]interface{}{4, baseTime.Add(time.Second * 13)},
				),
			}),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wg := &WatermarkGenerator{
				source:    tt.fields.source,
				timeField: tt.fields.timeField,
				offset:    tt.fields.offset,
			}

			stateStorage := storage.GetTestStorage(t)

			tx := stateStorage.BeginTransaction()
			ctx := storage.InjectStateTransaction(ctx, tx)

			got, _, err := wg.Get(ctx, tt.args.variables, execution.GetRawStreamID())
			if (err != nil) != tt.wantErr {
				t.Errorf("WatermarkGenerator.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			want, _, err := tt.want.Get(ctx, tt.args.variables, execution.GetRawStreamID())
			if err != nil {
				t.Errorf("WatermarkGenerator.Get() error = %v", err)
				return
			}

			err = execution.AreStreamsEqual(ctx, got, want)
			if err != nil {
				t.Errorf("WatermarkGenerator.Get() AreStreamsEqual error = %v", err)
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWatermarkGeneratorStream_GetWatermark(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2019, 9, 3, 12, 0, 0, 0, time.UTC)

	variables := octosql.NewVariables(map[octosql.VariableName]octosql.Value{
		"offset": octosql.MakeDuration(time.Second * 5),
	})

	source := execution.NewDummyNode([]*execution.Record{
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"id", "time"},
			[]interface{}{1, baseTime.Add(time.Second * 10)},
		),
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"id", "time"},
			[]interface{}{2, baseTime},
		),
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"id", "time"},
			[]interface{}{3, baseTime.Add(time.Second * 13)},
		),
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"id", "time"},
			[]interface{}{4, baseTime.Add(time.Second * 8)},
		),
	})
	timeField := octosql.NewVariableName("time")
	offset := execution.NewVariable(octosql.NewVariableName("offset"))

	wg := &WatermarkGenerator{
		source:    source,
		timeField: timeField,
		offset:    offset,
	}

	stateStorage := storage.GetTestStorage(t)

	tx := stateStorage.BeginTransaction()
	ctx = storage.InjectStateTransaction(ctx, tx)

	src, execOutput, err := wg.Get(ctx, variables, execution.GetRawStreamID())
	if err != nil {
		t.Errorf("WatermarkGenerator.Get() error = %v", err)
		return
	}

	assert.Equal(t, src, execOutput.WatermarkSource)

	ws := execOutput.WatermarkSource

	ExpectWatermarkValue(t, ctx, ws, tx, time.Time{})

	NextRecord(t, ctx, src) // curTime = 10, maxTime = 10

	ExpectWatermarkValue(t, ctx, ws, tx, baseTime.Add(time.Second*5)) // maxTime = 10 - 5

	NextRecord(t, ctx, src) // curTime = 0, maxTime = 10

	ExpectWatermarkValue(t, ctx, ws, tx, baseTime.Add(time.Second*5)) // maxTime = 10 - 5

	NextRecord(t, ctx, src) // curTime = 13, maxTime = 13

	ExpectWatermarkValue(t, ctx, ws, tx, baseTime.Add(time.Second*8)) // maxTime = 13 - 5

	NextRecord(t, ctx, src) // curTime = 8, maxTime = 13

	ExpectWatermarkValue(t, ctx, ws, tx, baseTime.Add(time.Second*8)) // maxTime = 13 - 5
}
