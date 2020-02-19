package trigger

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v2"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestWatermarkTrigger(t *testing.T) {
	ctx := context.Background()
	db, err := badger.Open(badger.DefaultOptions("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.RemoveAll("test")
	}()

	badgerStorage := storage.NewBadgerStorage(db)
	watermark := time.Now()
	wt := NewWatermarkTrigger()

	// Simple

	RecordReceived(t, ctx, wt, badgerStorage, octosql.MakeInt(2), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, badgerStorage)

	watermark = watermark.Add(time.Second * 30)
	UpdateWatermark(t, ctx, wt, badgerStorage, watermark)

	ExpectNoFire(t, ctx, wt, badgerStorage)

	watermark = watermark.Add(time.Minute)
	UpdateWatermark(t, ctx, wt, badgerStorage, watermark)

	ExpectFire(t, ctx, wt, badgerStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, wt, badgerStorage)

	// Key update

	RecordReceived(t, ctx, wt, badgerStorage, octosql.MakeInt(2), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, badgerStorage)

	watermark = watermark.Add(time.Second * 45)
	UpdateWatermark(t, ctx, wt, badgerStorage, watermark)

	ExpectNoFire(t, ctx, wt, badgerStorage)

	RecordReceived(t, ctx, wt, badgerStorage, octosql.MakeInt(2), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, badgerStorage)

	watermark = watermark.Add(time.Second * 45)
	UpdateWatermark(t, ctx, wt, badgerStorage, watermark)

	ExpectNoFire(t, ctx, wt, badgerStorage)

	watermark = watermark.Add(time.Second * 45)
	UpdateWatermark(t, ctx, wt, badgerStorage, watermark)

	ExpectFire(t, ctx, wt, badgerStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, wt, badgerStorage)

	// Two keys

	RecordReceived(t, ctx, wt, badgerStorage, octosql.MakeInt(2), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, badgerStorage)

	watermark = watermark.Add(time.Second * 30)
	UpdateWatermark(t, ctx, wt, badgerStorage, watermark)

	ExpectNoFire(t, ctx, wt, badgerStorage)

	RecordReceived(t, ctx, wt, badgerStorage, octosql.MakeInt(3), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, badgerStorage)

	watermark = watermark.Add(time.Second * 15)
	UpdateWatermark(t, ctx, wt, badgerStorage, watermark)

	ExpectNoFire(t, ctx, wt, badgerStorage)

	RecordReceived(t, ctx, wt, badgerStorage, octosql.MakeInt(2), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, badgerStorage)

	watermark = watermark.Add(time.Second * 50)
	UpdateWatermark(t, ctx, wt, badgerStorage, watermark)

	ExpectFire(t, ctx, wt, badgerStorage, octosql.MakeInt(3))

	ExpectNoFire(t, ctx, wt, badgerStorage)

	watermark = watermark.Add(time.Second * 20)
	UpdateWatermark(t, ctx, wt, badgerStorage, watermark)

	ExpectFire(t, ctx, wt, badgerStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, wt, badgerStorage)

	// Two keys Trigger at once

	RecordReceived(t, ctx, wt, badgerStorage, octosql.MakeInt(2), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, badgerStorage)

	watermark = watermark.Add(time.Second * 10)
	UpdateWatermark(t, ctx, wt, badgerStorage, watermark)

	ExpectNoFire(t, ctx, wt, badgerStorage)

	RecordReceived(t, ctx, wt, badgerStorage, octosql.MakeInt(3), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, badgerStorage)

	watermark = watermark.Add(time.Minute * 2)
	UpdateWatermark(t, ctx, wt, badgerStorage, watermark)

	ExpectFire(t, ctx, wt, badgerStorage, octosql.MakeInt(2), octosql.MakeInt(3))

	ExpectNoFire(t, ctx, wt, badgerStorage)

	// Key fired

	RecordReceived(t, ctx, wt, badgerStorage, octosql.MakeInt(2), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, badgerStorage)

	watermark = watermark.Add(time.Second * 30)
	UpdateWatermark(t, ctx, wt, badgerStorage, watermark)

	ExpectNoFire(t, ctx, wt, badgerStorage)

	RecordReceived(t, ctx, wt, badgerStorage, octosql.MakeInt(3), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, badgerStorage)

	KeyFired(t, ctx, wt, badgerStorage, octosql.MakeInt(2))

	watermark = watermark.Add(time.Second * 45)
	UpdateWatermark(t, ctx, wt, badgerStorage, watermark)

	ExpectFire(t, ctx, wt, badgerStorage, octosql.MakeInt(2))

	watermark = watermark.Add(time.Second * 45)
	UpdateWatermark(t, ctx, wt, badgerStorage, watermark)

	ExpectFire(t, ctx, wt, badgerStorage, octosql.MakeInt(3))

	// Key fired after Trigger ready

	RecordReceived(t, ctx, wt, badgerStorage, octosql.MakeInt(2), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, badgerStorage)

	watermark = watermark.Add(time.Second * 30)
	UpdateWatermark(t, ctx, wt, badgerStorage, watermark)

	ExpectNoFire(t, ctx, wt, badgerStorage)

	RecordReceived(t, ctx, wt, badgerStorage, octosql.MakeInt(3), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, badgerStorage)

	watermark = watermark.Add(time.Second * 45)
	UpdateWatermark(t, ctx, wt, badgerStorage, watermark)

	KeyFired(t, ctx, wt, badgerStorage, octosql.MakeInt(2))

	ExpectFire(t, ctx, wt, badgerStorage, octosql.MakeInt(2))

	watermark = watermark.Add(time.Second * 45)
	UpdateWatermark(t, ctx, wt, badgerStorage, watermark)

	ExpectFire(t, ctx, wt, badgerStorage, octosql.MakeInt(3))
}
