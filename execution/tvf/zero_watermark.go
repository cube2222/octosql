package tvf

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/streaming/storage"
)

type ZeroWatermark struct {
	source execution.Node
}

func NewZeroWatermark(source execution.Node) *ZeroWatermark {
	return &ZeroWatermark{
		source: source,
	}
}

func (w *ZeroWatermark) Document() docs.Documentation {
	panic("implement me")
}

func (w *ZeroWatermark) Get(ctx context.Context, variables octosql.Variables, streamID *execution.StreamID) (execution.RecordStream, error) {
	tx := storage.GetStateTransactionFromContext(ctx)

	sourceStreamID, err := execution.GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakePhantom())
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get source stream ID")
	}

	source, err := w.source.Get(ctx, variables, sourceStreamID)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get source")
	}

	return &ZeroWatermarkStream{
		source: source,
	}, nil
}

type ZeroWatermarkStream struct {
	source execution.RecordStream
}

func (s *ZeroWatermarkStream) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	return time.Time{}, nil
}

func (s *ZeroWatermarkStream) Next(ctx context.Context) (*execution.Record, error) {
	return s.source.Next(ctx)
}

func (s *ZeroWatermarkStream) Close() error {
	return s.source.Close()
}
