package execution

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cube2222/octosql/octosql"
)

func TestCountingTrigger(t *testing.T) {
	trigger := NewCountingTriggerPrototype(2)()
	trigger.KeyReceived(GroupKey{octosql.NewInt(2), octosql.NewInt(3)})
	assert.Empty(t, trigger.Poll())
	trigger.KeyReceived(GroupKey{octosql.NewInt(2), octosql.NewInt(3)})
	polled := trigger.Poll()
	assert.Len(t, polled, 1)
	assert.Equal(t, polled[0], GroupKey{octosql.NewInt(2), octosql.NewInt(3)})
}
