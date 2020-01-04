package streaming

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/stretchr/testify/assert"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func RecordReceived(t *testing.T, ctx context.Context, trigger Trigger, badgerStorage *storage.BadgerStorage, key octosql.Value, eventTime time.Time) {
	tx := badgerStorage.BeginTransaction()
	_, err := trigger.PollKeyToFire(ctx, tx)
	assert.Equal(t, ErrNoKeyToFire, err)
	err = trigger.RecordReceived(ctx, tx, key, eventTime)
	assert.Nil(t, err)
	assert.Nil(t, tx.Commit())
}

func ExpectFire(t *testing.T, ctx context.Context, trigger Trigger, badgerStorage *storage.BadgerStorage, key octosql.Value) {
	tx := badgerStorage.BeginTransaction()
	k, err := trigger.PollKeyToFire(ctx, tx)
	assert.Equal(t, key, k)
	assert.Nil(t, err)
	assert.Nil(t, tx.Commit())
}

func ExpectNoFire(t *testing.T, ctx context.Context, trigger Trigger, badgerStorage *storage.BadgerStorage) {
	tx := badgerStorage.BeginTransaction()
	_, err := trigger.PollKeyToFire(ctx, tx)
	assert.Equal(t, ErrNoKeyToFire, err)
	assert.Nil(t, tx.Commit())
}

func UpdateWatermark(t *testing.T, ctx context.Context, trigger Trigger, badgerStorage *storage.BadgerStorage, watermark time.Time) {
	tx := badgerStorage.BeginTransaction()
	err := trigger.UpdateWatermark(ctx, tx, watermark)
	assert.Nil(t, err)
	assert.Nil(t, tx.Commit())
}

func KeyFired(t *testing.T, ctx context.Context, trigger Trigger, badgerStorage *storage.BadgerStorage, key octosql.Value) {
	tx := badgerStorage.BeginTransaction()
	err := trigger.KeyFired(ctx, tx, key)
	assert.Nil(t, err)
	assert.Nil(t, tx.Commit())
}

func TestCountingTrigger(t *testing.T) {
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
	ct := NewCountingTrigger(3)

	RecordReceived(t, ctx, ct, badgerStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, ct, badgerStorage)

	RecordReceived(t, ctx, ct, badgerStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, ct, badgerStorage)

	RecordReceived(t, ctx, ct, badgerStorage, octosql.MakeInt(3), time.Time{})

	ExpectNoFire(t, ctx, ct, badgerStorage)

	RecordReceived(t, ctx, ct, badgerStorage, octosql.MakeInt(3), time.Time{})

	ExpectFire(t, ctx, ct, badgerStorage, octosql.MakeInt(3))

	ExpectNoFire(t, ctx, ct, badgerStorage)
}

func TestCountingTrigger_KeyFired(t *testing.T) {
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
	ct := NewCountingTrigger(3)

	RecordReceived(t, ctx, ct, badgerStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, ct, badgerStorage)

	RecordReceived(t, ctx, ct, badgerStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, ct, badgerStorage)

	RecordReceived(t, ctx, ct, badgerStorage, octosql.MakeInt(3), time.Time{})

	ExpectNoFire(t, ctx, ct, badgerStorage)

	RecordReceived(t, ctx, ct, badgerStorage, octosql.MakeInt(3), time.Time{})

	ExpectNoFire(t, ctx, ct, badgerStorage)

	KeyFired(t, ctx, ct, badgerStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, ct, badgerStorage)

	RecordReceived(t, ctx, ct, badgerStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, ct, badgerStorage)

	RecordReceived(t, ctx, ct, badgerStorage, octosql.MakeInt(3), time.Time{})

	ExpectFire(t, ctx, ct, badgerStorage, octosql.MakeInt(3))

	ExpectNoFire(t, ctx, ct, badgerStorage)

	RecordReceived(t, ctx, ct, badgerStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, ct, badgerStorage)

	RecordReceived(t, ctx, ct, badgerStorage, octosql.MakeInt(2), time.Time{})

	ExpectFire(t, ctx, ct, badgerStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, ct, badgerStorage)

	RecordReceived(t, ctx, ct, badgerStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, ct, badgerStorage)

	RecordReceived(t, ctx, ct, badgerStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, ct, badgerStorage)

	RecordReceived(t, ctx, ct, badgerStorage, octosql.MakeInt(2), time.Time{})

	KeyFired(t, ctx, ct, badgerStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, ct, badgerStorage)
}

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

	// Two keys trigger at once

	RecordReceived(t, ctx, wt, badgerStorage, octosql.MakeInt(2), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, badgerStorage)

	watermark = watermark.Add(time.Second * 10)
	UpdateWatermark(t, ctx, wt, badgerStorage, watermark)

	ExpectNoFire(t, ctx, wt, badgerStorage)

	RecordReceived(t, ctx, wt, badgerStorage, octosql.MakeInt(3), watermark.Add(time.Minute))

	ExpectNoFire(t, ctx, wt, badgerStorage)

	watermark = watermark.Add(time.Minute * 2)
	UpdateWatermark(t, ctx, wt, badgerStorage, watermark)

	ExpectFire(t, ctx, wt, badgerStorage, octosql.MakeInt(2))

	ExpectFire(t, ctx, wt, badgerStorage, octosql.MakeInt(3))

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

	// Key fired after trigger ready

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
