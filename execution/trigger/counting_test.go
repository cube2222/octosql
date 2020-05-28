package trigger

import (
	"context"
	"testing"
	"time"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

func TestCountingTrigger(t *testing.T) {
	ctx := context.Background()
	stateStorage := storage.GetTestStorage(t)
	ct := NewCountingTrigger(3)

	RecordReceived(t, ctx, ct, stateStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, ct, stateStorage)

	RecordReceived(t, ctx, ct, stateStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, ct, stateStorage)

	RecordReceived(t, ctx, ct, stateStorage, octosql.MakeInt(3), time.Time{})

	ExpectNoFire(t, ctx, ct, stateStorage)

	RecordReceived(t, ctx, ct, stateStorage, octosql.MakeInt(3), time.Time{})

	ExpectNoFire(t, ctx, ct, stateStorage)

	RecordReceived(t, ctx, ct, stateStorage, octosql.MakeInt(2), time.Time{})

	ExpectFire(t, ctx, ct, stateStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, ct, stateStorage)

	RecordReceived(t, ctx, ct, stateStorage, octosql.MakeInt(3), time.Time{})

	ExpectFire(t, ctx, ct, stateStorage, octosql.MakeInt(3))

	ExpectNoFire(t, ctx, ct, stateStorage)
}

func TestCountingTrigger_KeyFired(t *testing.T) {
	ctx := context.Background()
	stateStorage := storage.GetTestStorage(t)
	ct := NewCountingTrigger(3)

	RecordReceived(t, ctx, ct, stateStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, ct, stateStorage)

	RecordReceived(t, ctx, ct, stateStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, ct, stateStorage)

	RecordReceived(t, ctx, ct, stateStorage, octosql.MakeInt(3), time.Time{})

	ExpectNoFire(t, ctx, ct, stateStorage)

	RecordReceived(t, ctx, ct, stateStorage, octosql.MakeInt(3), time.Time{})

	ExpectNoFire(t, ctx, ct, stateStorage)

	KeyFired(t, ctx, ct, stateStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, ct, stateStorage)

	RecordReceived(t, ctx, ct, stateStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, ct, stateStorage)

	RecordReceived(t, ctx, ct, stateStorage, octosql.MakeInt(3), time.Time{})

	ExpectFire(t, ctx, ct, stateStorage, octosql.MakeInt(3))

	ExpectNoFire(t, ctx, ct, stateStorage)

	RecordReceived(t, ctx, ct, stateStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, ct, stateStorage)

	RecordReceived(t, ctx, ct, stateStorage, octosql.MakeInt(2), time.Time{})

	ExpectFire(t, ctx, ct, stateStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, ct, stateStorage)

	RecordReceived(t, ctx, ct, stateStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, ct, stateStorage)

	RecordReceived(t, ctx, ct, stateStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, ct, stateStorage)

	RecordReceived(t, ctx, ct, stateStorage, octosql.MakeInt(2), time.Time{})

	KeyFired(t, ctx, ct, stateStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, ct, stateStorage)
}
