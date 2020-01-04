package trigger

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

var ErrNoKeyToFire = errors.New("no record to send")

type Trigger interface {
	RecordReceived(ctx context.Context, tx storage.StateTransaction, key octosql.Value, eventTime time.Time) error
	UpdateWatermark(ctx context.Context, tx storage.StateTransaction, watermark time.Time) error
	PollKeyToFire(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error)
	KeyFired(ctx context.Context, tx storage.StateTransaction, key octosql.Value) error
}
