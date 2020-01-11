package streaming

import (
	"context"
	"log"
	"time"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/streaming/storage"
)

type RecordStorePrint struct {
}

func (p *RecordStorePrint) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, record *execution.Record) error {
	log.Printf("AddRecord(%d, %s)", inputIndex, record.Show())
	return nil
}

func (p *RecordStorePrint) Next(ctx context.Context, tx storage.StateTransaction) (*execution.Record, error) {
	panic("unreachable")
}

func (p *RecordStorePrint) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	log.Printf("UpdateWatermark(%v)", watermark)
	return nil
}

func (p *RecordStorePrint) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	panic("unreachable")
}

func (p *RecordStorePrint) MarkEndOfStream(ctx context.Context, tx storage.StateTransaction) error {
	log.Printf("MarkEndOfStream()")
	return nil
}

func (p *RecordStorePrint) Close() error {
	panic("unreachable")
}
