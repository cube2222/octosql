package kafka

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/storage"
)

type KafkaMessage struct {
	Key, Value string
}

func TestRecordStream_Next(t *testing.T) {
	streamID := execution.NewStreamID("test")

	tests := []struct {
		topic        string
		decodeAsJSON bool
		messages     []KafkaMessage
		want         []*execution.Record
		wantErr      bool
	}{
		{
			topic: "topic0",
			messages: []KafkaMessage{
				{
					Key:   "key0",
					Value: "value0",
				},
				{
					Key:   "key1",
					Value: "value1",
				},
				{
					Key:   "key2",
					Value: "value2",
				},
				{
					Key:   "key3",
					Value: "value3",
				},
			},
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{octosql.NewVariableName("e.key"), octosql.NewVariableName("e.offset"), octosql.NewVariableName("e.value")},
					[]interface{}{"key0", 0, "value0"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamID, 0)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{octosql.NewVariableName("e.key"), octosql.NewVariableName("e.offset"), octosql.NewVariableName("e.value")},
					[]interface{}{"key1", 1, "value1"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamID, 1)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{octosql.NewVariableName("e.key"), octosql.NewVariableName("e.offset"), octosql.NewVariableName("e.value")},
					[]interface{}{"key2", 2, "value2"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamID, 2)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{octosql.NewVariableName("e.key"), octosql.NewVariableName("e.offset"), octosql.NewVariableName("e.value")},
					[]interface{}{"key3", 3, "value3"},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamID, 3)),
				),
			},
		},
		{
			decodeAsJSON: true,
			topic:        "topic1",
			messages: []KafkaMessage{
				{
					Key:   "key0",
					Value: `{"id": 0, "color": "red"}`,
				},
				{
					Key:   "key1",
					Value: `{"id": 1, "color": invalid_json}`,
				},
				{
					Key:   "key2",
					Value: `{"id": 2, "wheels": 3}`,
				},
			},
			want: []*execution.Record{
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{octosql.NewVariableName("e.key"), octosql.NewVariableName("e.offset"), octosql.NewVariableName("e.color"), octosql.NewVariableName("e.id")},
					[]interface{}{"key0", 0, "red", 0.0},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamID, 0)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{octosql.NewVariableName("e.key"), octosql.NewVariableName("e.offset"), octosql.NewVariableName("e.value")},
					[]interface{}{"key1", 1, `{"id": 1, "color": invalid_json}`},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamID, 1)),
				),
				execution.NewRecordFromSliceWithNormalize(
					[]octosql.VariableName{octosql.NewVariableName("e.key"), octosql.NewVariableName("e.offset"), octosql.NewVariableName("e.id"), octosql.NewVariableName("e.wheels")},
					[]interface{}{"key2", 2, 2.0, 3.0},
					execution.WithID(execution.NewRecordIDFromStreamIDWithOffset(streamID, 2)),
				),
			},
		},
	}
	for _, tt := range tests {
		t.Run("kafka test", func(t *testing.T) {
			ctx := context.Background()
			stateStorage := storage.GetTestStorage(t)

			brokers := []string{"localhost:9092"}

			kafkaMessages := make([]kafka.Message, len(tt.messages))
			for i, msg := range tt.messages {
				kafkaMessages[i] = kafka.Message{
					Key:   []byte(msg.Key),
					Value: []byte(msg.Value),
				}
			}

			log.Println(t.Name())
			writerConfig := kafka.WriterConfig{
				Brokers: brokers,
				Topic:   tt.topic,
				Dialer: &kafka.Dialer{
					Timeout:   10 * time.Second,
					DualStack: true,
				},
				BatchSize: 1,
				Async:     false,
			}
			w := kafka.NewWriter(writerConfig)

			var err error
			for err = w.WriteMessages(ctx, kafkaMessages...); err == kafka.LeaderNotAvailable; err = w.WriteMessages(ctx, kafkaMessages...) {
				t.Log("waiting for topic leader to emerge")
				time.Sleep(time.Second)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			node := &DataSource{
				brokers:      brokers,
				topic:        tt.topic,
				partition:    0,
				batchSize:    1,
				decodeAsJSON: tt.decodeAsJSON,
				alias:        "e",
				stateStorage: stateStorage,
			}

			rs := execution.GetTestStream(t, stateStorage, octosql.NoVariables(), node, execution.GetTestStreamWithStreamID(streamID))

			tx := stateStorage.BeginTransaction()
			want, _, err := execution.NewDummyNode(tt.want).Get(storage.InjectStateTransaction(ctx, tx), octosql.NoVariables(), execution.GetRawStreamID())
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			if err := execution.AreStreamsEqualNoOrderingWithCount(context.Background(), stateStorage, rs, want, len(tt.want)); err != nil {
				t.Errorf("Streams aren't equal: %v", err)
				return
			}

			if err := rs.Close(ctx, stateStorage); err != nil {
				t.Errorf("Couldn't close kafka stream: %v", err)
				return
			}
		})
	}
}
