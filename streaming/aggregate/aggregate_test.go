package aggregate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
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
	case octosql.TypeString:
		assert.Equal(t, expected.AsString(), val.AsString())
	default:
		panic("unreachable")
	}

	assert.Nil(t, err)
}

func ExpectZeroValue(t *testing.T, ctx context.Context, aggr Aggregate, tx storage.StateTransaction) {
	val, _ := aggr.GetValue(ctx, tx)
	assert.NotNil(t, val)
	assert.Equal(t, octosql.ZeroValue().GetType(), val.GetType())
}

func AddValue(t *testing.T, ctx context.Context, aggr Aggregate, tx storage.StateTransaction, value octosql.Value) {
	err := aggr.AddValue(ctx, tx, value)
	assert.Nil(t, err)
}

func RetractValue(t *testing.T, ctx context.Context, aggr Aggregate, tx storage.StateTransaction, value octosql.Value) {
	err := aggr.RetractValue(ctx, tx, value)
	assert.Nil(t, err)
}
