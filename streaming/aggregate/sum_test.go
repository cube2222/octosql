package aggregate

import (
	"context"
	"os"
	"testing"

	"github.com/dgraph-io/badger"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestSumInt(t *testing.T) {
	ctx := context.Background()
	db, err := badger.Open(badger.DefaultOptions("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.RemoveAll("test")
	}()

	prefix := []byte("sum")

	badgerStorage := storage.NewBadgerStorage(db)
	tx := badgerStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewSumAggregate()

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(15))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(27))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(13))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(14))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// Mixed
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(4))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(7))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(3))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(4))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))
}

func TestSumFloat(t *testing.T) {
	ctx := context.Background()
	db, err := badger.Open(badger.DefaultOptions("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.RemoveAll("test")
	}()

	prefix := []byte("sum")

	badgerStorage := storage.NewBadgerStorage(db)
	tx := badgerStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewSumAggregate()

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeFloat(1.2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(1.2))

	AddValue(t, ctx, aggr, tx, octosql.MakeFloat(2.0))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(3.2))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeFloat(1.2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(2.0))

	RetractValue(t, ctx, aggr, tx, octosql.MakeFloat(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// Mixed
	AddValue(t, ctx, aggr, tx, octosql.MakeFloat(2.123))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(2.123))

	AddValue(t, ctx, aggr, tx, octosql.MakeFloat(0.5))

	RetractValue(t, ctx, aggr, tx, octosql.MakeFloat(0.5))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(2.123))

	AddValue(t, ctx, aggr, tx, octosql.MakeFloat(0.222))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(2.345))

	RetractValue(t, ctx, aggr, tx, octosql.MakeFloat(2.123))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(0.222))

	RetractValue(t, ctx, aggr, tx, octosql.MakeFloat(0.222))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))
}

func TestSumDuration(t *testing.T) {
	ctx := context.Background()
	db, err := badger.Open(badger.DefaultOptions("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.RemoveAll("test")
	}()

	prefix := []byte("sum")

	badgerStorage := storage.NewBadgerStorage(db)
	tx := badgerStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewSumAggregate()

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeDuration(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeDuration(1))

	AddValue(t, ctx, aggr, tx, octosql.MakeDuration(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeDuration(3))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeDuration(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeDuration(2))

	RetractValue(t, ctx, aggr, tx, octosql.MakeDuration(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// Mixed
	AddValue(t, ctx, aggr, tx, octosql.MakeDuration(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeDuration(2))

	AddValue(t, ctx, aggr, tx, octosql.MakeDuration(12))

	RetractValue(t, ctx, aggr, tx, octosql.MakeDuration(12))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeDuration(2))

	AddValue(t, ctx, aggr, tx, octosql.MakeDuration(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeDuration(3))

	RetractValue(t, ctx, aggr, tx, octosql.MakeDuration(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeDuration(2))

	RetractValue(t, ctx, aggr, tx, octosql.MakeDuration(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))
}
