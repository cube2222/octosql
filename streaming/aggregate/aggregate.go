package aggregate

import (
	"context"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

type Aggregate interface {
	AddValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error
	RetractValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error
	GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error)
	String() string
}
