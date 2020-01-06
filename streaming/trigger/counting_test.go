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

	ExpectNoFire(t, ctx, ct, badgerStorage)

	RecordReceived(t, ctx, ct, badgerStorage, octosql.MakeInt(2), time.Time{})

	ExpectFire(t, ctx, ct, badgerStorage, octosql.MakeInt(2))

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
