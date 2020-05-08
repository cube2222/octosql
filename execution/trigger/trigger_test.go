package trigger

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

func RecordReceived(t *testing.T, ctx context.Context, trigger Trigger, stateStorage storage.Storage, key octosql.Value, eventTime time.Time) {
	tx := stateStorage.BeginTransaction()
	keys, err := trigger.PollKeysToFire(ctx, tx, 1000000)
	assert.Nil(t, keys)
	assert.Nil(t, err)
	err = trigger.RecordReceived(ctx, tx, key, eventTime)
	assert.Nil(t, err)
	assert.Nil(t, tx.Commit())
}

func ExpectFire(t *testing.T, ctx context.Context, trigger Trigger, stateStorage storage.Storage, keys ...octosql.Value) {
	tx := stateStorage.BeginTransaction()
	k, err := trigger.PollKeysToFire(ctx, tx, 1000000)
	assert.Equal(t, keys, k)
	assert.Nil(t, err)
	assert.Nil(t, tx.Commit())
}

func ExpectNoFire(t *testing.T, ctx context.Context, trigger Trigger, stateStorage storage.Storage) {
	tx := stateStorage.BeginTransaction()
	keys, err := trigger.PollKeysToFire(ctx, tx, 1000000)
	assert.Nil(t, keys)
	assert.Nil(t, err)
	assert.Nil(t, tx.Commit())
}

func UpdateWatermark(t *testing.T, ctx context.Context, trigger Trigger, stateStorage storage.Storage, watermark time.Time) {
	tx := stateStorage.BeginTransaction()
	err := trigger.UpdateWatermark(ctx, tx, watermark)
	assert.Nil(t, err)
	assert.Nil(t, tx.Commit())
}

func KeyFired(t *testing.T, ctx context.Context, trigger Trigger, stateStorage storage.Storage, keys ...octosql.Value) {
	tx := stateStorage.BeginTransaction()
	err := trigger.KeysFired(ctx, tx, keys)
	assert.Nil(t, err)
	assert.Nil(t, tx.Commit())
}
