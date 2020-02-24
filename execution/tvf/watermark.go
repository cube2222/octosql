package tvf

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/docs"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/streaming/storage"
)

type Watermark struct {
	source    execution.Node
	timeField octosql.VariableName
}

func NewWatermark(source execution.Node, timeField octosql.VariableName) *Watermark {
	return &Watermark{
		source:    source,
		timeField: timeField,
	}
}

func (w *Watermark) Document() docs.Documentation {
	panic("implement me")
}

func (w *Watermark) Get(ctx context.Context, variables octosql.Variables, streamID *execution.StreamID) (execution.RecordStream, error) {
	tx := storage.GetStateTransactionFromContext(ctx)

	sourceStreamID, err := execution.GetSourceStreamID(tx.WithPrefix(streamID.AsPrefix()), octosql.MakePhantom())
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get source stream ID")
	}

	source, err := w.source.Get(ctx, variables, sourceStreamID)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get source")
	}

	return &WatermarkStream{
		source:    source,
		timeField: w.timeField,
	}, nil
}

type WatermarkStream struct {
	source    execution.RecordStream
	timeField octosql.VariableName
}

var watermarkPrefix = []byte("$watermark$")

func (s *WatermarkStream) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	watermarkStorage := storage.NewValueState(tx.WithPrefix(watermarkPrefix))

	var currentWatermark octosql.Value
	err := watermarkStorage.Get(&currentWatermark)
	if err == storage.ErrNotFound {
		currentWatermark = octosql.MakeTime(time.Time{})
	} else if err != nil {
		return time.Time{}, errors.Wrap(err, "couldn't get current watermark from storage")
	}

	return currentWatermark.AsTime(), nil
}

func (s *WatermarkStream) Next(ctx context.Context) (*execution.Record, error) {
	srcRecord, err := s.source.Next(ctx)
	if err != nil {
		if err == execution.ErrEndOfStream {
			return nil, execution.ErrEndOfStream
		}
		return nil, errors.Wrap(err, "couldn't get source record")
	}

	// TODO - ustaw nowa wartosc watermarka
	timeValue := srcRecord.Value(s.timeField)
	if timeValue.GetType() != octosql.TypeTime {
		return nil, fmt.Errorf("couldn't get time field '%v' as time, got: %v", s.timeField.String(), srcRecord.Value(s.timeField))
	}

	currentWatermark, err := s.GetWatermark(ctx, tx) // TODO - nie mam tu transakcji - zrobic nowa (?)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get current watermark value")
	}

	if timeValue.AsTime().After(currentWatermark) { // time in current record is bigger than current watermark - update it
		watermarkStorage := storage.NewValueState(tx.WithPrefix(watermarkPrefix)) // TODO - same

		err := watermarkStorage.Set(&timeValue)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't set new watermark value in storage")
		}
	}

	return srcRecord, nil
}

func (s *WatermarkStream) Close() error {
	return s.source.Close()
}
