package streaming

import (
	"context"
	"time"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/streaming/storage"
)

type WatermarkRecordStream struct {
	stream       []WatermarkRecordStreamEntry
	index        int
	curWatermark time.Time
}

func (w *WatermarkRecordStream) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	if w.index < len(w.stream) && w.stream[w.index].watermark != nil {
		w.curWatermark = *w.stream[w.index].watermark
		w.index++
	}
	return w.curWatermark, nil
}

func (w *WatermarkRecordStream) Next(ctx context.Context) (*execution.Record, error) {
	for {
		if w.index == len(w.stream) {
			return nil, execution.ErrEndOfStream
		}
		if w.stream[w.index].watermark != nil {
			w.curWatermark = *w.stream[w.index].watermark
			w.index++
		} else {
			w.index++
			return w.stream[w.index-1].record, nil
		}
	}
}

func (w *WatermarkRecordStream) Close(ctx context.Context) error {
	return nil
}

type WatermarkRecordStreamEntry struct {
	record    *execution.Record
	watermark *time.Time
}
