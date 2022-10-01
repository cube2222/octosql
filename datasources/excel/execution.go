package excel

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/valyala/fastjson/fastfloat"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/execution/files"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type DatasourceExecuting struct {
	path           string
	fields         []physical.SchemaField
	fileFieldNames []string
	header         bool
	separator      rune
}

func (d *DatasourceExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	f, err := files.OpenLocalFile(ctx, d.path)
	if err != nil {
		return fmt.Errorf("couldn't open local file: %w", err)
	}
	defer f.Close()

	usedColumns := map[string]bool{}
	for i := range d.fields {
		usedColumns[d.fields[i].Name] = true
	}

	decoder := csv.NewReader(f)
	decoder.Comma = d.separator
	decoder.ReuseRecord = true
	if d.header {
		_, err := decoder.Read()
		if err != nil {
			return fmt.Errorf("couldn't decode csv header row: %w", err)
		}
	}

	indicesToRead := make([]int, 0)
	for i := range d.fileFieldNames {
		if usedColumns[d.fileFieldNames[i]] {
			indicesToRead = append(indicesToRead, i)
		}
	}

	for {
		row, err := decoder.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("couldn't decode message: %w", err)
		}

		values := make([]octosql.Value, len(indicesToRead))
		for i, columnIndex := range indicesToRead {
			str := row[columnIndex]
			if str == "" {
				values[i] = octosql.NewNull()
				continue
			}

			if octosql.Int.Is(d.fields[i].Type) == octosql.TypeRelationIs {
				integer, err := fastfloat.ParseInt64(str)
				if err == nil {
					values[i] = octosql.NewInt(int(integer))
					continue
				}
			}

			if octosql.Float.Is(d.fields[i].Type) == octosql.TypeRelationIs {
				float, err := fastfloat.Parse(str)
				if err == nil {
					values[i] = octosql.NewFloat(float)
					continue
				}
			}

			if octosql.Boolean.Is(d.fields[i].Type) == octosql.TypeRelationIs {
				b, err := strconv.ParseBool(str)
				if err == nil {
					values[i] = octosql.NewBoolean(b)
					continue
				}
			}

			if octosql.Time.Is(d.fields[i].Type) == octosql.TypeRelationIs {
				t, err := time.Parse(time.RFC3339Nano, str)
				if err == nil {
					values[i] = octosql.NewTime(t)
					continue
				}
			}

			values[i] = octosql.NewString(str)
		}

		if err := produce(ProduceFromExecutionContext(ctx), NewRecord(values, false, time.Time{})); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}

	return nil
}
