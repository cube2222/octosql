package nodes

import (
	"github.com/cube2222/octosql/arrowexec/execution"
)

type TestNode struct {
	Records []execution.Record
}

func (t *TestNode) Run(ctx execution.Context, produce execution.ProduceFunc) error {
	for i := range t.Records {
		if err := produce(execution.ProduceContext(ctx), t.Records[i]); err != nil {
			return err
		}
	}
	return nil
}
