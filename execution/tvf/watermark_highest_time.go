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

type WatermarkHighestTime struct {
	source execution.Node
}

func NewWatermarkHighestTime(source execution.Node) *WatermarkHighestTime {
	return &WatermarkHighestTime{source: source}
}

func (wg *WatermarkHighestTime) Document() docs.Documentation {
	panic("implement me")
}

func (wg *WatermarkHighestTime) Get(ctx context.Context, variables octosql.Variables, streamID *execution.StreamID) (execution.RecordStream, error) {
	tx := storage.GetStateTransactionFromContext(ctx)

	sourceStreamID, err := execution.GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakePhantom())
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get source stream ID")
	}

	source, err := wg.source.Get(ctx, variables, sourceStreamID)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get source")
	}

	return &WatermarkHighestTimeStream{
		source: source,
	}, nil
}

type WatermarkHighestTimeStream struct {
	source execution.RecordStream
}

func (s *WatermarkHighestTimeStream) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	panic("implement me")
}

func (s *WatermarkHighestTimeStream) Next(ctx context.Context) (*execution.Record, error) {
	srcRecord, err := s.source.Next(ctx)
	if err != nil {
		if err == execution.ErrEndOfStream {
			return nil, execution.ErrEndOfStream
		}
		return nil, errors.Wrap(err, "couldn't get source record")
	}

	// TODO - ustaw nowa wartosc watermarka

	return srcRecord, nil
}

func (s *WatermarkHighestTimeStream) Close() error {
	return s.source.Close()
}
