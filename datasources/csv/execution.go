package csv

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

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

	decoder := csv.NewReader(bufio.NewReaderSize(f, 4096*1024))
	decoder.Comma = ','
	decoder.ReuseRecord = true
	_, err = decoder.Read()
	if err != nil {
		return fmt.Errorf("couldn't decode csv header row: %w", err)
	}

	for {
		row, err := decoder.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("couldn't decode message: %w", err)
		}

		values := make([]octosql.Value, len(d.fields))
		for i := range row {
			str := row[i]
			integer, err := strconv.ParseInt(str, 10, 64)
			if err == nil {
				values[i] = octosql.NewInt(int(integer))
				continue
			}

			float, err := strconv.ParseFloat(str, 64)
			if err == nil {
				values[i] = octosql.NewFloat(float)
				continue
			}

			b, err := strconv.ParseBool(str)
			if err == nil {
				values[i] = octosql.NewBoolean(b)
				continue
			}

			t, err := time.Parse(time.RFC3339Nano, str)
			if err == nil {
				values[i] = octosql.NewTime(t)
				continue
			}

			values[i] = octosql.NewString(str)
		}

		if err := produce(ProduceFromExecutionContext(ctx), NewRecord(values, false, time.Time{})); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}

	return nil
}
