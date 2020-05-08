package trigger

import (
	"context"
	"testing"
	"time"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

func TestWatermarkTrigger(t *testing.T) {
	ctx := context.Background()
	stateStorage := storage.GetTestStorage(t)
	watermark := time.Now()
	wt := NewWatermarkTrigger()

	// Simple

	RecordReceived(t, ctx, wt, stateStorage, octosql.MakeInt(2), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, stateStorage)

	watermark = watermark.Add(time.Second * 30)
	UpdateWatermark(t, ctx, wt, stateStorage, watermark)

	ExpectNoFire(t, ctx, wt, stateStorage)

	watermark = watermark.Add(time.Minute)
	UpdateWatermark(t, ctx, wt, stateStorage, watermark)

	ExpectFire(t, ctx, wt, stateStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, wt, stateStorage)

	// Key update

	RecordReceived(t, ctx, wt, stateStorage, octosql.MakeInt(2), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, stateStorage)

	watermark = watermark.Add(time.Second * 45)
	UpdateWatermark(t, ctx, wt, stateStorage, watermark)

	ExpectNoFire(t, ctx, wt, stateStorage)

	RecordReceived(t, ctx, wt, stateStorage, octosql.MakeInt(2), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, stateStorage)

	watermark = watermark.Add(time.Second * 45)
	UpdateWatermark(t, ctx, wt, stateStorage, watermark)

	ExpectNoFire(t, ctx, wt, stateStorage)

	watermark = watermark.Add(time.Second * 45)
	UpdateWatermark(t, ctx, wt, stateStorage, watermark)

	ExpectFire(t, ctx, wt, stateStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, wt, stateStorage)

	// Two keys

	RecordReceived(t, ctx, wt, stateStorage, octosql.MakeInt(2), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, stateStorage)

	watermark = watermark.Add(time.Second * 30)
	UpdateWatermark(t, ctx, wt, stateStorage, watermark)

	ExpectNoFire(t, ctx, wt, stateStorage)

	RecordReceived(t, ctx, wt, stateStorage, octosql.MakeInt(3), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, stateStorage)

	watermark = watermark.Add(time.Second * 15)
	UpdateWatermark(t, ctx, wt, stateStorage, watermark)

	ExpectNoFire(t, ctx, wt, stateStorage)

	RecordReceived(t, ctx, wt, stateStorage, octosql.MakeInt(2), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, stateStorage)

	watermark = watermark.Add(time.Second * 50)
	UpdateWatermark(t, ctx, wt, stateStorage, watermark)

	ExpectFire(t, ctx, wt, stateStorage, octosql.MakeInt(3))

	ExpectNoFire(t, ctx, wt, stateStorage)

	watermark = watermark.Add(time.Second * 20)
	UpdateWatermark(t, ctx, wt, stateStorage, watermark)

	ExpectFire(t, ctx, wt, stateStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, wt, stateStorage)

	// Two keys Trigger at once

	RecordReceived(t, ctx, wt, stateStorage, octosql.MakeInt(2), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, stateStorage)

	watermark = watermark.Add(time.Second * 10)
	UpdateWatermark(t, ctx, wt, stateStorage, watermark)

	ExpectNoFire(t, ctx, wt, stateStorage)

	RecordReceived(t, ctx, wt, stateStorage, octosql.MakeInt(3), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, stateStorage)

	watermark = watermark.Add(time.Minute * 2)
	UpdateWatermark(t, ctx, wt, stateStorage, watermark)

	ExpectFire(t, ctx, wt, stateStorage, octosql.MakeInt(2), octosql.MakeInt(3))

	ExpectNoFire(t, ctx, wt, stateStorage)

	// Key fired

	RecordReceived(t, ctx, wt, stateStorage, octosql.MakeInt(2), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, stateStorage)

	watermark = watermark.Add(time.Second * 30)
	UpdateWatermark(t, ctx, wt, stateStorage, watermark)

	ExpectNoFire(t, ctx, wt, stateStorage)

	RecordReceived(t, ctx, wt, stateStorage, octosql.MakeInt(3), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, stateStorage)

	KeyFired(t, ctx, wt, stateStorage, octosql.MakeInt(2))

	watermark = watermark.Add(time.Second * 45)
	UpdateWatermark(t, ctx, wt, stateStorage, watermark)

	ExpectFire(t, ctx, wt, stateStorage, octosql.MakeInt(2))

	watermark = watermark.Add(time.Second * 45)
	UpdateWatermark(t, ctx, wt, stateStorage, watermark)

	ExpectFire(t, ctx, wt, stateStorage, octosql.MakeInt(3))

	// Key fired after Trigger ready

	RecordReceived(t, ctx, wt, stateStorage, octosql.MakeInt(2), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, stateStorage)

	watermark = watermark.Add(time.Second * 30)
	UpdateWatermark(t, ctx, wt, stateStorage, watermark)

	ExpectNoFire(t, ctx, wt, stateStorage)

	RecordReceived(t, ctx, wt, stateStorage, octosql.MakeInt(3), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, stateStorage)

	watermark = watermark.Add(time.Second * 45)
	UpdateWatermark(t, ctx, wt, stateStorage, watermark)

	KeyFired(t, ctx, wt, stateStorage, octosql.MakeInt(2))

	ExpectFire(t, ctx, wt, stateStorage, octosql.MakeInt(2))

	watermark = watermark.Add(time.Second * 45)
	UpdateWatermark(t, ctx, wt, stateStorage, watermark)

	ExpectFire(t, ctx, wt, stateStorage, octosql.MakeInt(3))
}
