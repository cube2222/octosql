package aggregate

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

type First struct {
}

func (agg *First) AddValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	panic("not implemented yet")
}

func (agg *First) RetractValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	panic("not implemented yet")
}

func (agg *First) GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	panic("not implemented yet")
}

func (agg *First) String() string {
	return "first"
}
