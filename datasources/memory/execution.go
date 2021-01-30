package memory

import (
	"fmt"

	. "github.com/cube2222/octosql/execution"
)

type Datasource struct {
	Records []Record
}

func (d *Datasource) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	for _, record := range d.Records {
		if err := produce(ProduceFromExecutionContext(ctx), record); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}
	return nil
}
