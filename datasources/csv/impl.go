package csv

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

func Creator(name string, options map[string]string) (physical.DatasourceImplementation, physical.Schema, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, physical.Schema{}, fmt.Errorf("couldn't open file: %w", err)
	}
	defer f.Close()

	header := true
	if headerStr, ok := options["header"]; ok {
		header, err = strconv.ParseBool(headerStr)
		if err != nil {
			return nil, physical.Schema{}, errors.Wrap(err, "couldn't parse header option, must be true or false")
		}
	}

	decoder := csv.NewReader(f)
	decoder.Comma = ','
	decoder.ReuseRecord = true
	var fieldNames []string
	if header {
		row, err := decoder.Read()
		if err != nil {
			return nil, physical.Schema{}, fmt.Errorf("couldn't decode csv header row: %w", err)
		}
		fieldNames = make([]string, len(row))
		copy(fieldNames, row)
	}

	fields := make([]octosql.Type, len(fieldNames))
	filled := make([]bool, len(fieldNames))
	for i := 0; i < 10; i++ {
		row, err := decoder.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, physical.Schema{}, fmt.Errorf("couldn't decode message: %w", err)
		}

		if fieldNames == nil {
			fieldNames = make([]string, len(row))
			for i := range row {
				fieldNames[i] = fmt.Sprintf("column_%d", i)
			}
			fields = make([]octosql.Type, len(fieldNames))
			filled = make([]bool, len(fieldNames))
		}

		for i := range row {
			str := row[i]
			if str == "" {
				if !filled[i] {
					fields[i] = octosql.Null
					filled[i] = true
				} else if !fields[i].Equals(octosql.Null) {
					fields[i] = octosql.TypeSum(fields[i], octosql.Null)
				}
				continue
			}

			_, err := strconv.ParseInt(str, 10, 64)
			if err == nil {
				if !filled[i] {
					fields[i] = octosql.Int
					filled[i] = true
				} else if !fields[i].Equals(octosql.Float) {
					fields[i] = octosql.TypeSum(fields[i], octosql.Int)
				}
				continue
			}

			_, err = strconv.ParseFloat(str, 64)
			if err == nil {
				if !filled[i] {
					fields[i] = octosql.Float
					filled[i] = true
				} else if fields[i].Equals(octosql.Int) {
					fields[i] = octosql.Float
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
			path:           name,
			header:         header,
			fileFieldNames: fieldNames,
		},
		physical.NewSchema(schemaFields, -1, physical.WithNoRetractions(true)),
		nil
}

type impl struct {
	path           string
	header         bool
	fileFieldNames []string
}

func (i *impl) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (execution.Node, error) {
	return &DatasourceExecuting{
		path:           i.path,
		fields:         schema.Fields,
		header:         i.header,
		fileFieldNames: i.fileFieldNames,
	}, nil
}

func (i *impl) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}
