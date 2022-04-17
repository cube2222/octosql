package lines

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/edsrzf/mmap-go"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type DatasourceExecuting struct {
	path, separator string
	fields          []physical.SchemaField
}

func (d *DatasourceExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	f, err := os.Open(d.path)
	if err != nil {
		return fmt.Errorf("couldn't open file: %w", err)
	}
	defer f.Close()

	data, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		return fmt.Errorf("couldn't memory map file: %w", err)
	}

	curOffset := 0
	line := 0
	for {
		endOfLineOffset := bytes.Index(data[curOffset:], []byte(d.separator))

		values := make([]octosql.Value, len(d.fields))
		for i := range d.fields {
			switch d.fields[i].Name {
			case "number":
				values[i] = octosql.NewInt(line)
			case "text":
				if endOfLineOffset != -1 {
					values[i] = octosql.NewString(string(data[curOffset : curOffset+endOfLineOffset]))
				} else {
					values[i] = octosql.NewString(string(data[curOffset:]))
				}
			}
		}

		if err := produce(ProduceFromExecutionContext(ctx), NewRecord(values, false, time.Time{})); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
		line++
		if endOfLineOffset == -1 {
			break
		}
		curOffset = curOffset + endOfLineOffset + 1
	}
	return nil
}
