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

type StaticClock struct {
	t time.Time
}

func (clock *StaticClock) SetTime(newTime time.Time) {
	clock.t = newTime
}

func (clock *StaticClock) Advance(dur time.Duration) {
	clock.t = clock.t.Add(dur)
}

func (clock *StaticClock) Now() time.Time {
	return clock.t
}

func TestDelayTrigger(t *testing.T) {
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
	clock := &StaticClock{}
	now := time.Now()
	clock.SetTime(now)
	dt := NewDelayTrigger(time.Minute, clock.Now)

	// Simple

	RecordReceived(t, ctx, dt, badgerStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, dt, badgerStorage)

	clock.Advance(time.Second * 30)

	ExpectNoFire(t, ctx, dt, badgerStorage)

	clock.Advance(time.Minute)

	ExpectFire(t, ctx, dt, badgerStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, dt, badgerStorage)

	// Key update

	RecordReceived(t, ctx, dt, badgerStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, dt, badgerStorage)

	clock.Advance(time.Second * 45)

	ExpectNoFire(t, ctx, dt, badgerStorage)

	RecordReceived(t, ctx, dt, badgerStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, dt, badgerStorage)

	clock.Advance(time.Second * 45)

	ExpectNoFire(t, ctx, dt, badgerStorage)

	clock.Advance(time.Second * 45)

	ExpectFire(t, ctx, dt, badgerStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, dt, badgerStorage)

	// Two keys

	RecordReceived(t, ctx, dt, badgerStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, dt, badgerStorage)

	clock.Advance(time.Second * 30)

	ExpectNoFire(t, ctx, dt, badgerStorage)

	RecordReceived(t, ctx, dt, badgerStorage, octosql.MakeInt(3), time.Time{})

	ExpectNoFire(t, ctx, dt, badgerStorage)

	clock.Advance(time.Second * 15)

	ExpectNoFire(t, ctx, dt, badgerStorage)

	RecordReceived(t, ctx, dt, badgerStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, dt, badgerStorage)

	clock.Advance(time.Second * 50)

	ExpectFire(t, ctx, dt, badgerStorage, octosql.MakeInt(3))

	ExpectNoFire(t, ctx, dt, badgerStorage)

	clock.Advance(time.Second * 20)

	ExpectFire(t, ctx, dt, badgerStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, dt, badgerStorage)

	// Two keys trigger at once

	RecordReceived(t, ctx, dt, badgerStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, dt, badgerStorage)

	clock.Advance(time.Second * 10)

	ExpectNoFire(t, ctx, dt, badgerStorage)

	RecordReceived(t, ctx, dt, badgerStorage, octosql.MakeInt(3), time.Time{})

	ExpectNoFire(t, ctx, dt, badgerStorage)

	clock.Advance(time.Minute * 2)

	ExpectFire(t, ctx, dt, badgerStorage, octosql.MakeInt(2))

	ExpectFire(t, ctx, dt, badgerStorage, octosql.MakeInt(3))

	ExpectNoFire(t, ctx, dt, badgerStorage)

	// Key fired

	RecordReceived(t, ctx, dt, badgerStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, dt, badgerStorage)

	clock.Advance(time.Second * 30)

	ExpectNoFire(t, ctx, dt, badgerStorage)

	RecordReceived(t, ctx, dt, badgerStorage, octosql.MakeInt(3), time.Time{})

	ExpectNoFire(t, ctx, dt, badgerStorage)

	KeyFired(t, ctx, dt, badgerStorage, octosql.MakeInt(2))

	clock.Advance(time.Second * 45)

	ExpectNoFire(t, ctx, dt, badgerStorage)

	clock.Advance(time.Second * 45)

	ExpectFire(t, ctx, dt, badgerStorage, octosql.MakeInt(3))

	// Key fired after trigger ready

	RecordReceived(t, ctx, dt, badgerStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, dt, badgerStorage)

	clock.Advance(time.Second * 30)

	ExpectNoFire(t, ctx, dt, badgerStorage)

	RecordReceived(t, ctx, dt, badgerStorage, octosql.MakeInt(3), time.Time{})

	ExpectNoFire(t, ctx, dt, badgerStorage)

	clock.Advance(time.Second * 45)

	KeyFired(t, ctx, dt, badgerStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, dt, badgerStorage)

	clock.Advance(time.Second * 45)

	ExpectFire(t, ctx, dt, badgerStorage, octosql.MakeInt(3))
}
