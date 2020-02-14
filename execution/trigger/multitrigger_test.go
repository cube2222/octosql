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

func TestMultiTrigger(t *testing.T) {
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
	ct := NewCountingTrigger(2)
	clock := &StaticClock{}
	clock.SetTime(time.Now())
	dt := NewDelayTrigger(time.Minute, clock.Now)
	mt := NewMultiTrigger(ct, dt)

	/*
		RecordReceived(t, ctx, mt, badgerStorage, octosql.MakeInt(2), time.Time{})

		ExpectNoFire(t, ctx, mt, badgerStorage)

		KeyFired(t, ctx, mt, badgerStorage, octosql.MakeInt(2))

		ExpectFire(t, ctx, mt, badgerStorage, octosql.MakeInt(3))
	*/

	ExpectNoFire(t, ctx, mt, badgerStorage)

	RecordReceived(t, ctx, mt, badgerStorage, octosql.MakeInt(2), clock.Now())

	ExpectNoFire(t, ctx, mt, badgerStorage)

	RecordReceived(t, ctx, mt, badgerStorage, octosql.MakeInt(2), clock.Now())

	ExpectFire(t, ctx, mt, badgerStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, mt, badgerStorage)

	clock.Advance(time.Minute * 2)

	ExpectNoFire(t, ctx, mt, badgerStorage)

	RecordReceived(t, ctx, mt, badgerStorage, octosql.MakeInt(2), clock.Now())

	ExpectNoFire(t, ctx, mt, badgerStorage)

	clock.Advance(time.Minute * 2)

	ExpectFire(t, ctx, mt, badgerStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, mt, badgerStorage)

	RecordReceived(t, ctx, mt, badgerStorage, octosql.MakeInt(2), clock.Now())

	ExpectNoFire(t, ctx, mt, badgerStorage)
}
