package aggregate

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

var currentLastPrefix = []byte("$current_last$")

type Last struct {
}

func NewLastAggregate() *Last {
	return &Last{}
}

func (agg *Last) AddValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	panic("implement me")
}

func (agg *Last) RetractValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	panic("implement me")
}

func (agg *Last) GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	panic("implement me")
}

func (agg *Last) String() string {
	return "last"
}
