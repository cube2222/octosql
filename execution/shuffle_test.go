package execution

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

type VariableBasedStrategyPrototype struct {
	name octosql.VariableName
}

func (s *VariableBasedStrategyPrototype) Get(ctx context.Context, variables octosql.Variables) (ShuffleStrategy, error) {
	return &VariableBasedStrategy{
		name: s.name,
	}, nil
}

type VariableBasedStrategy struct {
	name octosql.VariableName
}

func (s *VariableBasedStrategy) CalculatePartition(ctx context.Context, record *Record, outputs int) (int, error) {
	return record.Value(s.name).AsInt(), nil
}

func TestShuffle(t *testing.T) {
	fieldNames := []octosql.VariableName{
		octosql.NewVariableName("age"),
		octosql.NewVariableName("something"),
	}

	tests := []struct {
		name             string
		outputPartitions int
		output           Node

		want    []Node
		wantErr bool
	}{
		{
			name:             "one output multiple source",
			outputPartitions: 1,
			output: NewShuffle(1, NewConstantStrategyPrototype(0), []Node{
				NewDummyNode(
					[]*Record{
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{4, "test2"},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{3, "test3"},
						),
					},
				),
				NewDummyNode(
					[]*Record{
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{5, "test"},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{3, "test33"},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{2, "test2"},
						),
					},
				),
				NewDummyNode(
					[]*Record{
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{5, "test"},
						),
					},
				),
			},
			),
			want: []Node{
				NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{4, "test2"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{3, "test3"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{5, "test"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{3, "test33"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{2, "test2"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{5, "test"},
					),
				}),
			},
			wantErr: false,
		},
		{
			name:             "multiple output one source",
			outputPartitions: 3,
			output: NewShuffle(3, &VariableBasedStrategyPrototype{name: octosql.NewVariableName("something")}, []Node{
				NewDummyNode(
					[]*Record{
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{4, 0},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{3, 1},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{5, 0},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{3, 2},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{2, 1},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{5, 0},
						),
					},
				),
			},
			),
			want: []Node{
				NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{4, 0},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{5, 0},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{5, 0},
					),
				}),
				NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{3, 1},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{2, 1},
					),
				}),
				NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{3, 2},
					),
				}),
			},
			wantErr: false,
		},
		{
			name:             "multiple output multiple source",
			outputPartitions: 3,
			output: NewShuffle(3, &VariableBasedStrategyPrototype{name: octosql.NewVariableName("something")}, []Node{
				NewDummyNode(
					[]*Record{
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{4, 0},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{3, 1},
						),
					},
				),
				NewDummyNode(
					[]*Record{
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{5, 0},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{3, 2},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{2, 1},
						),
						NewRecordFromSliceWithNormalize(
							fieldNames,
							[]interface{}{5, 0},
						),
					},
				),
			},
			),
			want: []Node{
				NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{4, 0},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{5, 0},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{5, 0},
					),
				}),
				NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{3, 1},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{2, 1},
					),
				}),
				NewDummyNode([]*Record{
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{3, 2},
					),
				}),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stateStorage := storage.GetTestStorage(t)

			ctx := context.Background()

			outputs := make([]Node, tt.outputPartitions)
			for i := 0; i < tt.outputPartitions; i++ {
				outputs[i] = tt.output
			}
			outputStreams, _, err := GetAndStartAllShuffles(ctx, stateStorage, NewStreamID("root"), outputs, octosql.NoVariables())
			if err != nil {
				t.Fatal(err)
			}

			tx := stateStorage.BeginTransaction()
			ctx = storage.InjectStateTransaction(ctx, tx)

			wantOutputStreams := make([]RecordStream, tt.outputPartitions)
			for i := 0; i < tt.outputPartitions; i++ {

				stream, _, err := tt.want[i].Get(ctx, octosql.NoVariables(), GetRawStreamID())
				if err != nil {
					t.Fatal(err)
				}
				wantOutputStreams[i] = stream
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			for i := 0; i < tt.outputPartitions; i++ {
				err := AreStreamsEqualNoOrdering(context.Background(), stateStorage, outputStreams[i], wantOutputStreams[i])
				if (err != nil) != tt.wantErr {
					t.Errorf("Shuffle.Next() error = %v, wantErr %v", err, tt.wantErr)
					return
				}

				if err := outputStreams[i].Close(ctx, stateStorage); err != nil {
					t.Errorf("Couldn't close output stream with id %v: %v", i, err)
					return
				}
				if err := wantOutputStreams[i].Close(ctx, stateStorage); err != nil {
					t.Errorf("Couldn't close wanted in_memory stream with id %v: %v", i, err)
					return
				}
			}
		})
	}
}

func TestShuffleMultiStage(t *testing.T) {
	fieldNames := []octosql.VariableName{
		octosql.NewVariableName("age"),
		octosql.NewVariableName("something"),
	}

	output := NewShuffle(
		5,
		NewKeyHashingStrategyPrototype([]Expression{NewVariable(octosql.NewVariableName("something"))}),
		[]Node{
			NewDummyNode(
				[]*Record{
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{4, "test2"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{3, "test3"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{5, "test"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{3, "test33"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{2, "test2"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{4, "test2"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{3, "test3"},
					),
				},
			),
			NewDummyNode(
				[]*Record{
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{5, "test"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{3, "test33"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{2, "test2"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{4, "test2"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{3, "test3"},
					),
				},
			),
			NewDummyNode(
				[]*Record{
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{5, "test"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{4, "test2"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{3, "test3"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{5, "test"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{3, "test33"},
					),
					NewRecordFromSliceWithNormalize(
						fieldNames,
						[]interface{}{2, "test2"},
					),
				},
			),
		},
	)
	output = NewShuffle(4,
		NewKeyHashingStrategyPrototype([]Expression{NewVariable(octosql.NewVariableName("something"))}),
		[]Node{
			output,
			output,
			output,
			output,
			output,
		},
	)
	output = NewShuffle(1,
		NewKeyHashingStrategyPrototype([]Expression{NewVariable(octosql.NewVariableName("something"))}),
		[]Node{
			output,
			output,
			output,
			output,
		},
	)
	output = NewShuffle(3,
		NewKeyHashingStrategyPrototype([]Expression{NewVariable(octosql.NewVariableName("something"))}),
		[]Node{
			output,
		},
	)
	output = NewShuffle(4,
		NewKeyHashingStrategyPrototype([]Expression{NewVariable(octosql.NewVariableName("something"))}),
		[]Node{
			output,
			output,
			output,
		},
	)
	output = NewShuffle(1,
		NewKeyHashingStrategyPrototype([]Expression{NewVariable(octosql.NewVariableName("something"))}),
		[]Node{
			output,
			output,
			output,
			output,
		},
	)
	want := NewDummyNode(
		[]*Record{
			NewRecordFromSliceWithNormalize(
				fieldNames,
				[]interface{}{4, "test2"},
			),
			NewRecordFromSliceWithNormalize(
				fieldNames,
				[]interface{}{3, "test3"},
			),
			NewRecordFromSliceWithNormalize(
				fieldNames,
				[]interface{}{5, "test"},
			),
			NewRecordFromSliceWithNormalize(
				fieldNames,
				[]interface{}{3, "test33"},
			),
			NewRecordFromSliceWithNormalize(
				fieldNames,
				[]interface{}{2, "test2"},
			),
			NewRecordFromSliceWithNormalize(
				fieldNames,
				[]interface{}{4, "test2"},
			),
			NewRecordFromSliceWithNormalize(
				fieldNames,
				[]interface{}{3, "test3"},
			),
			NewRecordFromSliceWithNormalize(
				fieldNames,
				[]interface{}{5, "test"},
			),
			NewRecordFromSliceWithNormalize(
				fieldNames,
				[]interface{}{3, "test33"},
			),
			NewRecordFromSliceWithNormalize(
				fieldNames,
				[]interface{}{2, "test2"},
			),
			NewRecordFromSliceWithNormalize(
				fieldNames,
				[]interface{}{4, "test2"},
			),
			NewRecordFromSliceWithNormalize(
				fieldNames,
				[]interface{}{3, "test3"},
			),
			NewRecordFromSliceWithNormalize(
				fieldNames,
				[]interface{}{5, "test"},
			),
			NewRecordFromSliceWithNormalize(
				fieldNames,
				[]interface{}{4, "test2"},
			),
			NewRecordFromSliceWithNormalize(
				fieldNames,
				[]interface{}{3, "test3"},
			),
			NewRecordFromSliceWithNormalize(
				fieldNames,
				[]interface{}{5, "test"},
			),
			NewRecordFromSliceWithNormalize(
				fieldNames,
				[]interface{}{3, "test33"},
			),
			NewRecordFromSliceWithNormalize(
				fieldNames,
				[]interface{}{2, "test2"},
			),
		},
	)

	stateStorage := storage.GetTestStorage(t)

	tx := stateStorage.BeginTransaction()
	ctx := storage.InjectStateTransaction(context.Background(), tx)

	outputStream, _, err := GetAndStartAllShuffles(ctx, stateStorage, NewStreamID("root"), []Node{output}, octosql.NoVariables())
	if err != nil {
		t.Fatal(err)
	}

	wantStream, _, err := want.Get(ctx, octosql.NoVariables(), GetRawStreamID())
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	err = AreStreamsEqualNoOrdering(context.Background(), stateStorage, outputStream[0], wantStream)
	if err != nil {
		t.Errorf("Shuffle.Next() error = %v", err)
		return
	}

	if err := outputStream[0].Close(ctx, stateStorage); err != nil {
		t.Errorf("Couldn't close output stream: %v", err)
		return
	}
	if err := wantStream.Close(ctx, stateStorage); err != nil {
		t.Errorf("Couldn't close wanted in_memory stream: %v", err)
		return
	}
}

func TestShuffleWatermarks(t *testing.T) {
	stateStorage := storage.GetTestStorage(t)

	tx := stateStorage.BeginTransaction()
	ctx := context.Background()
	ctx = storage.InjectStateTransaction(ctx, tx)

	shuffleID := &ShuffleID{
		Id: "test",
	}

	sender0 := NewShuffleSender(
		GetRawStreamID(),
		shuffleID,
		&VariableBasedStrategy{
			name: octosql.NewVariableName("partition"),
		},
		2,
		0,
	)
	sender1 := NewShuffleSender(
		GetRawStreamID(),
		shuffleID,
		&VariableBasedStrategy{
			name: octosql.NewVariableName("partition"),
		},
		2,
		1,
	)
	receiver0 := NewShuffleReceiver(
		GetRawStreamID(),
		shuffleID,
		2,
		0,
	)
	receiver1 := NewShuffleReceiver(
		GetRawStreamID(),
		shuffleID,
		2,
		1,
	)

	ExpectWatermarkValue(t, ctx, time.Time{}, receiver0)
	ExpectWatermarkValue(t, ctx, time.Time{}, receiver1)

	PropagateWatermarks(t, ctx, []IntermediateRecordStore{sender0, sender1}, []RecordStream{receiver0, receiver1})

	ExpectWatermarkValue(t, ctx, time.Time{}, receiver0)
	ExpectWatermarkValue(t, ctx, time.Time{}, receiver1)

	now := time.Now().UTC()
	UpdateWatermark(t, ctx, now, sender0)
	now = now.Add(time.Second)
	UpdateWatermark(t, ctx, now, sender1)

	PropagateWatermarks(t, ctx, []IntermediateRecordStore{sender0, sender1}, []RecordStream{receiver0, receiver1})

	ExpectWatermarkValue(t, ctx, now.Add(-time.Second), receiver0)
	ExpectWatermarkValue(t, ctx, now.Add(-time.Second), receiver1)

	UpdateWatermark(t, ctx, now, sender0)

	PropagateWatermarks(t, ctx, []IntermediateRecordStore{sender0, sender1}, []RecordStream{receiver0, receiver1})

	ExpectWatermarkValue(t, ctx, now, receiver0)
	ExpectWatermarkValue(t, ctx, now, receiver1)

	PropagateWatermarks(t, ctx, []IntermediateRecordStore{sender0, sender1}, []RecordStream{receiver0, receiver1})

	ExpectWatermarkValue(t, ctx, now, receiver0)
	ExpectWatermarkValue(t, ctx, now, receiver1)

	now = now.Add(time.Second)
	UpdateWatermark(t, ctx, now, sender0)
	UpdateWatermark(t, ctx, now, sender1)

	PropagateWatermarks(t, ctx, []IntermediateRecordStore{sender0, sender1}, []RecordStream{receiver0, receiver1})

	ExpectWatermarkValue(t, ctx, now, receiver0)
	ExpectWatermarkValue(t, ctx, now, receiver1)

	if err := sender0.Close(ctx, stateStorage); err != nil {
		t.Errorf("Couldn't close sender0: %v", err)
		return
	}
	if err := sender1.Close(ctx, stateStorage); err != nil {
		t.Errorf("Couldn't close sender1: %v", err)
		return
	}
}

func PropagateWatermarks(t *testing.T, ctx context.Context, senders []IntermediateRecordStore, receivers []RecordStream) {
	for i := range senders {
		for partition := range receivers {
			SendRecordToPartition(t, ctx, partition, senders[i])
		}
	}

	for i := range receivers {
		for range senders {
			NextRecord(t, ctx, receivers[i])
		}
		NoRecord(t, ctx, receivers[i])
	}
}

func ExpectWatermarkValue(t *testing.T, ctx context.Context, expected time.Time, ws WatermarkSource) {
	val, err := ws.GetWatermark(ctx, storage.GetStateTransactionFromContext(ctx))
	assert.Equal(t, expected, val)
	assert.Nil(t, err)
}

func NextRecord(t *testing.T, ctx context.Context, rs RecordStream) {
	_, err := rs.Next(ctx)
	assert.Nil(t, err)
}

func NoRecord(t *testing.T, ctx context.Context, rs RecordStream) {
	_, err := rs.Next(ctx)
	if err := GetErrWaitForChanges(err); err != nil {
		if err := err.Close(); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Fatalf("expected err wait for changes, got %v", err)
	}
}

func SendRecordToPartition(t *testing.T, ctx context.Context, partition int, irs IntermediateRecordStore) {
	err := irs.AddRecord(ctx, storage.GetStateTransactionFromContext(ctx), 0, NewRecordFromSlice(
		[]octosql.VariableName{octosql.NewVariableName("partition")},
		[]octosql.Value{octosql.MakeInt(partition)},
	))
	assert.Nil(t, err)
}

func UpdateWatermark(t *testing.T, ctx context.Context, watermark time.Time, irs IntermediateRecordStore) {
	err := irs.UpdateWatermark(ctx, storage.GetStateTransactionFromContext(ctx), watermark)
	assert.Nil(t, err)
}
