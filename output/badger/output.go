package badger

import (
	"context"
	"time"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/streaming/storage"
)

type Output struct {
}

func (o *Output) ReadyForMore(ctx context.Context, tx storage.StateTransaction) error {
	return nil
}

func (o *Output) AddRecord(ctx context.Context, tx storage.StateTransaction, inputIndex int, record *execution.Record) error {
	panic("implement me")
}

func (o *Output) Next(ctx context.Context, tx storage.StateTransaction) (*execution.Record, error) {
	panic("no next in output")
}

func (o *Output) UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error {
	panic("implement me")
}

func (o *Output) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	panic("implement me")
}

func (o *Output) MarkEndOfStream(ctx context.Context, tx storage.StateTransaction) error {
	panic("implement me")
}

func (o *Output) MarkError(ctx context.Context, tx storage.StateTransaction, err error) error {
	panic("implement me")
}

func (o *Output) Close() error {
	panic("implement me")
}
