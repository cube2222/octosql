package tvf

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/storage"
)

func ExpectWatermarkValue(t *testing.T, ctx context.Context, ws execution.WatermarkSource, tx storage.StateTransaction, expected time.Time) {
	val, err := ws.GetWatermark(ctx, tx)
	assert.Equal(t, expected, val)
	assert.Nil(t, err)
}

func NextRecord(t *testing.T, ctx context.Context, rs execution.RecordStream) {
	_, err := rs.Next(ctx)
	assert.Nil(t, err)
}
