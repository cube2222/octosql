package aggregates

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/storage"
)

var (
	eps = octosql.MakeFloat(0.00000001).AsFloat()
)

func ExpectValue(t *testing.T, ctx context.Context, aggr execution.Aggregate, tx storage.StateTransaction, expected octosql.Value) {
	val, err := aggr.GetValue(ctx, tx)
	assert.NotNil(t, val)
	assert.Equal(t, expected.GetType(), val.GetType())

	switch expected.GetType() {
	case octosql.TypeInt:
		assert.Equal(t, expected.AsInt(), val.AsInt())
	case octosql.TypeFloat:
		assert.Greater(t, expected.AsFloat(), val.AsFloat()-eps)
		assert.Less(t, expected.AsFloat(), val.AsFloat()+eps)
		//assert.Equal(t, expected.AsFloat(), val.AsFloat())
	case octosql.TypeBool:
		assert.Equal(t, expected.AsBool(), val.AsBool())
	case octosql.TypeString:
		assert.Equal(t, expected.AsString(), val.AsString())
	case octosql.TypeTime:
		assert.Equal(t, expected.AsTime(), val.AsTime())
	case octosql.TypeDuration:
		assert.Equal(t, expected.AsDuration(), val.AsDuration())
	case octosql.TypeTuple: // works only for basic types in tuple
		expectedSlice := expected.AsSlice()
		valSlice := val.AsSlice()

		assert.Equal(t, len(expectedSlice), len(valSlice))

		for i := range expectedSlice {
			assert.Equal(t, &expectedSlice[i].Value, &valSlice[i].Value)
		}

		//assert.Equal(t, expected.AsSlice(), val.AsSlice())
	default:
		panic("unreachable")
	}

	assert.Nil(t, err)
}

func ExpectZeroValue(t *testing.T, ctx context.Context, aggr execution.Aggregate, tx storage.StateTransaction) {
	val, _ := aggr.GetValue(ctx, tx)
	assert.NotNil(t, val)
	assert.Equal(t, octosql.ZeroValue().GetType(), val.GetType())
}

func AddValue(t *testing.T, ctx context.Context, aggr execution.Aggregate, tx storage.StateTransaction, value octosql.Value) {
	err := aggr.AddValue(ctx, tx, value)
	assert.Nil(t, err)
}

func AddValueError(t *testing.T, ctx context.Context, aggr execution.Aggregate, tx storage.StateTransaction, value octosql.Value) {
	err := aggr.AddValue(ctx, tx, value)
	assert.NotNil(t, err)
}

func RetractValue(t *testing.T, ctx context.Context, aggr execution.Aggregate, tx storage.StateTransaction, value octosql.Value) {
	err := aggr.RetractValue(ctx, tx, value)
	assert.Nil(t, err)
}

func RetractValueError(t *testing.T, ctx context.Context, aggr execution.Aggregate, tx storage.StateTransaction, value octosql.Value) {
	err := aggr.RetractValue(ctx, tx, value)
	assert.NotNil(t, err)
}
