package aggregate

import (
	"context"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
	"github.com/stretchr/testify/assert"
	"testing"
)

const eps = 0.00000001

func ExpectValue(t *testing.T, ctx context.Context, aggr Aggregate, tx storage.StateTransaction, expected octosql.Value) {
	val, err := aggr.GetValue(ctx, tx)
	assert.NotNil(t, val)
	assert.Equal(t, expected.GetType(), val.GetType())

	switch expected.GetType() {
	case octosql.TypeInt:
		assert.Equal(t, expected.AsInt(), val.AsInt())
	case octosql.TypeFloat:
		assert.Greater(t, val.AsFloat()+octosql.MakeFloat(eps).AsFloat(), expected.AsFloat())
		assert.Less(t, val.AsFloat()-octosql.MakeFloat(eps).AsFloat(), expected.AsFloat())
		//assert.Equal(t, expected.AsFloat(), val.AsFloat())
	case octosql.TypeDuration:
		assert.Equal(t, expected.AsDuration(), val.AsDuration())
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
