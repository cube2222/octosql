package aggregate

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

type Min struct {
}

func NewMinAggregate() *Min {
	return &Min{}
}

func (m Min) AddValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	panic("implement me")
}

func (m Min) RetractValue(ctx context.Context, tx storage.StateTransaction, value octosql.Value) error {
	panic("implement me")
}

func (m Min) GetValue(ctx context.Context, tx storage.StateTransaction) (octosql.Value, error) {
	panic("implement me")
}

func (m Min) String() string {
	panic("implement me")
}
