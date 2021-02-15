package csv

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

func Creator(name string) (physical.DatasourceImplementation, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, fmt.Errorf("couldn't open file: %w", err)
	}
	defer f.Close()

	decoder := csv.NewReader(f)
	decoder.Comma = ','
	decoder.ReuseRecord = true
	row, err := decoder.Read()
	if err != nil {
		return nil, fmt.Errorf("couldn't decode csv header row: %w", err)
	}
	fieldNames := make([]string, len(row))
	copy(fieldNames, row)

	fields := make([]octosql.Type, len(fieldNames))
	filled := make([]bool, len(fieldNames))
	for i := 0; i < 10; i++ {
		row, err = decoder.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("couldn't decode message: %w", err)
		}

		for i := range row {
			str := row[i]
			_, err := strconv.ParseInt(str, 10, 64)
			if err == nil {
				if !filled[i] {
					fields[i] = octosql.Int
					filled[i] = true
				} else {
					fields[i] = octosql.TypeSum(fields[i], octosql.Int)
				}
				continue
			}

			_, err = strconv.ParseFloat(str, 64)
			if err == nil {
				if !filled[i] {
					fields[i] = octosql.Float
					filled[i] = true
				} else {
					fields[i] = octosql.TypeSum(fields[i], octosql.Float)
				}
				continue
			}

			_, err = strconv.ParseBool(str)
			if err == nil {
				if !filled[i] {
					fields[i] = octosql.Boolean
					filled[i] = true
				} else {
					fields[i] = octosql.TypeSum(fields[i], octosql.Boolean)
				}
				continue
			}

			_, err = time.Parse(time.RFC3339Nano, str)
			if err == nil {
				if !filled[i] {
					fields[i] = octosql.Time
					filled[i] = true
				} else {
					fields[i] = octosql.TypeSum(fields[i], octosql.Time)
				}
				continue
			}

			if !filled[i] {
				fields[i] = octosql.String
				filled[i] = true
			} else {
				fields[i] = octosql.TypeSum(fields[i], octosql.String)
			}
		}
	}

	schemaFields := make([]physical.SchemaField, len(fields))
	for i := range fields {
		schemaFields[i] = physical.SchemaField{
			Name: fieldNames[i],
			Type: fields[i],
		}
	}

	return &impl{
		path:   name,
		schema: physical.NewSchema(schemaFields, -1),
	}, nil
}

type impl struct {
	path   string
	schema physical.Schema
}

func (i *impl) Schema() (physical.Schema, error) {
	return i.schema, nil
}

func (i *impl) Materialize(ctx context.Context, env physical.Environment, pushedDownPredicates []physical.Expression) (execution.Node, error) {
	return &DatasourceExecuting{
		path:   i.path,
		fields: i.schema.Fields,
	}, nil
}

func (i *impl) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected []physical.Expression, pushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}
