package memory

import (
	"fmt"
	"time"

	. "github.com/cube2222/octosql/execution"
)

type Datasource struct {
	Entries []Entry
}

type Entry struct {
	Record         Record
	WatermarkEntry bool
	Watermark      time.Time
}

func (d *Datasource) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	for _, entry := range d.Entries {
		if !entry.WatermarkEntry {
			if err := produce(ProduceFromExecutionContext(ctx), entry.Record); err != nil {
				return fmt.Errorf("couldn't produce record: %w", err)
			}
		} else {
			if err := metaSend(ProduceFromExecutionContext(ctx), MetadataMessage{
				Type:      MetadataMessageTypeWatermark,
				Watermark: entry.Watermark,
			}); err != nil {
				return fmt.Errorf("couldn't send metadata: %w", err)
			}
		}

	}
	return nil
}
