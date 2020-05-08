package trigger

import (
	"context"
	"testing"
	"time"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
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
	stateStorage := storage.GetTestStorage(t)
	clock := &StaticClock{}
	now := time.Now()
	clock.SetTime(now)
	dt := NewDelayTrigger(time.Minute, clock.Now)

	// Simple

	RecordReceived(t, ctx, dt, stateStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, dt, stateStorage)

	clock.Advance(time.Second * 30)

	ExpectNoFire(t, ctx, dt, stateStorage)

	clock.Advance(time.Minute)

	ExpectFire(t, ctx, dt, stateStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, dt, stateStorage)

	// Key update

	RecordReceived(t, ctx, dt, stateStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, dt, stateStorage)

	clock.Advance(time.Second * 45)

	ExpectNoFire(t, ctx, dt, stateStorage)

	RecordReceived(t, ctx, dt, stateStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, dt, stateStorage)

	clock.Advance(time.Second * 45)

	ExpectNoFire(t, ctx, dt, stateStorage)

	clock.Advance(time.Second * 45)

	ExpectFire(t, ctx, dt, stateStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, dt, stateStorage)

	// Two keys

	RecordReceived(t, ctx, dt, stateStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, dt, stateStorage)

	clock.Advance(time.Second * 30)

	ExpectNoFire(t, ctx, dt, stateStorage)

	RecordReceived(t, ctx, dt, stateStorage, octosql.MakeInt(3), time.Time{})

	ExpectNoFire(t, ctx, dt, stateStorage)

	clock.Advance(time.Second * 15)

	ExpectNoFire(t, ctx, dt, stateStorage)

	RecordReceived(t, ctx, dt, stateStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, dt, stateStorage)

	clock.Advance(time.Second * 50)

	ExpectFire(t, ctx, dt, stateStorage, octosql.MakeInt(3))

	ExpectNoFire(t, ctx, dt, stateStorage)

	clock.Advance(time.Second * 20)

	ExpectFire(t, ctx, dt, stateStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, dt, stateStorage)

	// Two keys Trigger at once

	RecordReceived(t, ctx, dt, stateStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, dt, stateStorage)

	clock.Advance(time.Second * 10)

	ExpectNoFire(t, ctx, dt, stateStorage)

	RecordReceived(t, ctx, dt, stateStorage, octosql.MakeInt(3), time.Time{})

	ExpectNoFire(t, ctx, dt, stateStorage)

	clock.Advance(time.Minute * 2)

	ExpectFire(t, ctx, dt, stateStorage, octosql.MakeInt(2), octosql.MakeInt(3))

	ExpectNoFire(t, ctx, dt, stateStorage)

	// Key fired

	RecordReceived(t, ctx, dt, stateStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, dt, stateStorage)

	clock.Advance(time.Second * 30)

	ExpectNoFire(t, ctx, dt, stateStorage)

	RecordReceived(t, ctx, dt, stateStorage, octosql.MakeInt(3), time.Time{})

	ExpectNoFire(t, ctx, dt, stateStorage)

	KeyFired(t, ctx, dt, stateStorage, octosql.MakeInt(2))

	clock.Advance(time.Second * 45)

	ExpectNoFire(t, ctx, dt, stateStorage)

	clock.Advance(time.Second * 45)

	ExpectFire(t, ctx, dt, stateStorage, octosql.MakeInt(3))

	// Key fired after Trigger ready

	RecordReceived(t, ctx, dt, stateStorage, octosql.MakeInt(2), time.Time{})

	ExpectNoFire(t, ctx, dt, stateStorage)

	clock.Advance(time.Second * 30)

	ExpectNoFire(t, ctx, dt, stateStorage)

	RecordReceived(t, ctx, dt, stateStorage, octosql.MakeInt(3), time.Time{})

	ExpectNoFire(t, ctx, dt, stateStorage)

	clock.Advance(time.Second * 45)

	KeyFired(t, ctx, dt, stateStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, dt, stateStorage)

	clock.Advance(time.Second * 45)

	ExpectFire(t, ctx, dt, stateStorage, octosql.MakeInt(3))
}
