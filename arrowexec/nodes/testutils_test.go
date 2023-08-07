package nodes

import (
	"github.com/cube2222/octosql/arrowexec/execution"
)

type TestNode struct {
	Records     []execution.Record
	Repetitions int
}

func (t *TestNode) Run(ctx execution.Context, produce execution.ProduceFunc) error {
	reps := 1
	if t.Repetitions > 0 {
		reps = t.Repetitions
	}

	for rep := 0; rep < reps; rep++ {
		for i := range t.Records {
			if err := produce(execution.ProduceContext{Context: ctx}, t.Records[i]); err != nil {
				return err
			}
		}
	}
	return nil
}
