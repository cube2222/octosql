package trigger

import (
	"context"
	"testing"
	"time"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

func TestMultiTrigger(t *testing.T) {
	ctx := context.Background()
	stateStorage := storage.GetTestStorage(t)
	ct := NewCountingTrigger(2)
	clock := &StaticClock{}
	clock.SetTime(time.Now())
	dt := NewDelayTrigger(time.Minute, clock.Now)
	mt := NewMultiTrigger(ct, dt)

	/*
		RecordReceived(t, ctx, mt, stateStorage, octosql.MakeInt(2), time.Time{})

		ExpectNoFire(t, ctx, mt, stateStorage)

		KeysFired(t, ctx, mt, stateStorage, octosql.MakeInt(2))

		ExpectFire(t, ctx, mt, stateStorage, octosql.MakeInt(3))
	*/

	ExpectNoFire(t, ctx, mt, stateStorage)

	RecordReceived(t, ctx, mt, stateStorage, octosql.MakeInt(2), clock.Now())

	ExpectNoFire(t, ctx, mt, stateStorage)

	RecordReceived(t, ctx, mt, stateStorage, octosql.MakeInt(2), clock.Now())

	ExpectFire(t, ctx, mt, stateStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, mt, stateStorage)

	clock.Advance(time.Minute * 2)

	ExpectNoFire(t, ctx, mt, stateStorage)

	RecordReceived(t, ctx, mt, stateStorage, octosql.MakeInt(2), clock.Now())

	ExpectNoFire(t, ctx, mt, stateStorage)

	clock.Advance(time.Minute * 2)

	ExpectFire(t, ctx, mt, stateStorage, octosql.MakeInt(2))

	ExpectNoFire(t, ctx, mt, stateStorage)

	RecordReceived(t, ctx, mt, stateStorage, octosql.MakeInt(2), clock.Now())

	ExpectNoFire(t, ctx, mt, stateStorage)
}
