package execution

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

type RecordStream interface {
	Next(ctx context.Context) (*octosql.Record, error)
	Close(ctx context.Context, storage storage.Storage) error
}

var ErrEndOfStream = errors.New("end of stream")
