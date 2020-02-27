package tvf

import (
	"context"
	"time"

	"github.com/cube2222/octosql/streaming/storage"
)

type ZeroWatermarkGenerator struct {
}

func (s *ZeroWatermarkGenerator) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	return time.Time{}, nil
}

func NewZeroWatermarkGenerator() *ZeroWatermarkGenerator {
	return &ZeroWatermarkGenerator{}
}

/*
func (w *ZeroWatermarkGenerator) Document() docs.Documentation {
	panic("implement me")
}

func (w *ZeroWatermarkGenerator) Get(ctx context.Context, variables octosql.Variables, streamID *execution.StreamID) (execution.RecordStream, *execution.ExecOutput, error) {
	tx := storage.GetStateTransactionFromContext(ctx)
	sourceStreamID, err := execution.GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakePhantom())
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source stream ID")
	}

	source, _, err := w.source.Get(ctx, variables, sourceStreamID) // we don't need execOutput here since we become new watermark source
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get source")
	}

	ws := &ZeroWatermarkGeneratorStream{
		source: source,
	}

	return ws, execution.NewExecOutput(ws), nil // watermark generator stream now indicates new watermark source
}

type ZeroWatermarkGeneratorStream struct {
	source execution.RecordStream
}

func (s *ZeroWatermarkGenerator) Next(ctx context.Context) (*execution.Record, error) {
	return s.source.Next(ctx)
}

func (s *ZeroWatermarkGenerator) Close() error {
	return s.source.Close()
}
*/
