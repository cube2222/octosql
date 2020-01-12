package aggregate

import (
	"context"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v2"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func TestAvgInt(t *testing.T) {
	ctx := context.Background()
	db, err := badger.Open(badger.DefaultOptions("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.RemoveAll("test")
	}()

	prefix := []byte("avg")

	badgerStorage := storage.NewBadgerStorage(db)
	tx := badgerStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewAverageAggregate()

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(1)) // Val: 1	Num: 1

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(1))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(3)) // Val: 4	Num: 2

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(2))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(8)) // Val: 12	Num: 3

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(4))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(2)) // Val: 14	Num: 4

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(3.5))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(8)) // Val: 6	Num: 3

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(2))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(3)) // Val: 3	Num: 2

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(1.5))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(1)) // Val: 2	Num: 1

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(2))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(2))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// Mixed
	AddValue(t, ctx, aggr, tx, octosql.MakeInt(3)) // Val: 3	Num: 1

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(3))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(8)) // Val: 11	Num: 2

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(5.5))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(3)) // Val: 8	Num: 1

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(8))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(41)) // Val: 49	Num: 2

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(24.5))

	AddValue(t, ctx, aggr, tx, octosql.MakeInt(1)) // Val: 50	Num: 3

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(16.6666666666))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(41)) // Val: 9	Num: 2

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(4.5))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(8)) // Val: 1	Num: 1

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(1))

	RetractValue(t, ctx, aggr, tx, octosql.MakeInt(1))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))
}

func TestAvgFloat(t *testing.T) {
	ctx := context.Background()
	db, err := badger.Open(badger.DefaultOptions("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.RemoveAll("test")
	}()

	prefix := []byte("avg")

	badgerStorage := storage.NewBadgerStorage(db)
	tx := badgerStorage.BeginTransaction().WithPrefix(prefix)

	aggr := NewAverageAggregate()

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))

	// AddValue
	AddValue(t, ctx, aggr, tx, octosql.MakeFloat(1.5)) // Val: 1.5	 Num: 1

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(1.5))

	AddValue(t, ctx, aggr, tx, octosql.MakeFloat(3.6)) // Val: 5.1	 Num: 2

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(2.55))

	AddValue(t, ctx, aggr, tx, octosql.MakeFloat(4.2)) // Val: 9.3	 Num: 3

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(3.1))

	AddValue(t, ctx, aggr, tx, octosql.MakeFloat(0.9)) // Val: 10.2 Num: 4

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(2.55))

	// RetractValue
	RetractValue(t, ctx, aggr, tx, octosql.MakeFloat(4.2)) // Val: 6	 Num: 3

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(2))

	RetractValue(t, ctx, aggr, tx, octosql.MakeFloat(3.6)) // Val: 2.4	 Num: 2

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(1.2))

	RetractValue(t, ctx, aggr, tx, octosql.MakeFloat(1.5)) // Val: 0.9	 Num: 1

	ExpectValue(t, ctx, aggr, tx, octosql.MakeFloat(0.9))

	RetractValue(t, ctx, aggr, tx, octosql.MakeFloat(0.9))

	ExpectValue(t, ctx, aggr, tx, octosql.MakeInt(0))
}
