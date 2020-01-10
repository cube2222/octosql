package aggregate

import (
	"context"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
	"github.com/stretchr/testify/assert"
	"testing"
)

func ExpectValue(t *testing.T, ctx context.Context, aggr Aggregate, tx storage.StateTransaction, expected octosql.Value) {
	val, err := aggr.GetValue(ctx, tx)
	assert.NotNil(t, val)
	assert.Equal(t, val.GetType(), expected.GetType())

	switch expected.GetType() {
	case octosql.TypeInt:
		assert.Equal(t, val.AsInt(), expected.AsInt())
	default:
		panic("unreachable")
	}

	assert.Nil(t, err)
}

func AddValue(t *testing.T, ctx context.Context, aggr Aggregate, tx storage.StateTransaction, value octosql.Value) {
	err := aggr.AddValue(ctx, tx, value)
	assert.Nil(t, err)
}

func RetractValue(t *testing.T, ctx context.Context, aggr Aggregate, tx storage.StateTransaction, value octosql.Value) {
	err := aggr.RetractValue(ctx, tx, value)
	assert.Nil(t, err)
}
