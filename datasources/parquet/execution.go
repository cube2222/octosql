package parquet

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/segmentio/parquet-go"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type DatasourceExecuting struct {
	path   string
	fields []physical.SchemaField
}

func (d *DatasourceExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	f, err := os.Open(d.path)
	if err != nil {
		return fmt.Errorf("couldn't open file: %w", err)
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("couldn't stat file: %w", err)
	}

	pf, err := parquet.OpenFile(f, stat.Size(), &parquet.FileConfig{
		SkipPageIndex:    true,
		SkipBloomFilters: true,
	})
	usedFields := make([]string, len(d.fields))
	for i := range usedFields {
		usedFields[i] = d.fields[i].Name
	}
	pf.Schema().MakeColumnReadRowFunc(usedFields)
	reconstruct := reconstructFuncOfSchemaFields(pf.Schema(), usedFields)

	var row parquet.Row
	pr := parquet.NewReader(pf)
	if len(usedFields) > 0 {
		for {
			row, err := pr.ReadRow(row)
			if err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("couldn't read row: %w", err)
			}
			var value octosql.Value
			if _, err := reconstruct(&value, levels{}, row); err != nil {
				return fmt.Errorf("couldn't reconstruct value from row: %w", err)
			}
			if err := produce(ProduceFromExecutionContext(ctx), NewRecord(value.Struct, false, time.Time{})); err != nil {
				return fmt.Errorf("couldn't produce value: %w", err)
			}
		}
	} else {
		rowCount := int(pr.NumRows())
		for i := 0; i < rowCount; i++ {
			if err := produce(ProduceFromExecutionContext(ctx), NewRecord([]octosql.Value{}, false, time.Time{})); err != nil {
				return fmt.Errorf("couldn't produce value: %w", err)
			}
		}
	}

	return nil
}
