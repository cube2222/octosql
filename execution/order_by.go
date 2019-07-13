package execution

import (
	"reflect"
	"sort"

	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type OrderDirection = string

const (
	Ascending  OrderDirection = "asc"
	Descending OrderDirection = "desc"
)

type OrderField struct {
	ColumnName octosql.VariableName
	Direction  OrderDirection
}

type OrderBy struct {
	Fields []OrderField
	Source Node
}

func NewOrderBy(fields []OrderField, source Node) *OrderBy {
	return &OrderBy{
		Fields: fields,
		Source: source,
	}
}

func isSorteable(x octosql.Value) bool {
	switch x.(type) {
	case octosql.Int:
		return true
	case octosql.Float:
		return true
	case octosql.String:
		return true
	default:
		return false
	}
}

func validateOrderField(records []*Record, field OrderField) error {
	if len(records) == 0 { /* we can easily order an empty stream */
		return nil
	}

	colName := field.ColumnName

	for _, record := range records {
		value := record.Value(colName)
		if value == nil {
			return errors.Errorf("one of the records has no mapping for %v", colName)
		}

		if reflect.TypeOf(value) != reflect.TypeOf(records[0].Value(field.ColumnName)) {
			return errors.Errorf("two records have mismatched types for column %v", colName)
		}
	} /* now that we have checked that types match we can check if it's sortable */

	if !isSorteable(records[0].Value(colName)) {
		return errors.Errorf("type of this column is not sortable")
	}

	return nil
}

func validateRecords(records []*Record, orderFields []OrderField) error {
	for _, field := range orderFields {
		err := validateOrderField(records, field)
		if err != nil {
			return err
		}
	}

	return nil
}

func compare(x, y octosql.Value) (int, error) {
	switch x := x.(type) {
	case octosql.Int:
		y, ok := y.(octosql.Int)
		if !ok {
			return 0, errors.Errorf("type mismatch between values")
		}

		if x == y {
			return 0, nil
		} else if x < y {
			return -1, nil
		}

		return 1, nil
	case octosql.Float:
		y, ok := y.(octosql.Float)
		if !ok {
			return 0, errors.Errorf("type mismatch between values")
		}

		if x == y {
			return 0, nil
		} else if x < y {
			return -1, nil
		}

		return 1, nil
	case octosql.String:
		y, ok := y.(octosql.String)
		if !ok {
			return 0, errors.Errorf("type mismatch between values")
		}

		if x == y {
			return 0, nil
		} else if x < y {
			return -1, nil
		}

		return 1, nil
	case octosql.Time:
		y, ok := y.(octosql.Time)
		if !ok {
			return 0, errors.Errorf("type mismatch between values")
		}

		if x == y {
			return 0, nil
		} else if x.Time().Before(y.Time()) {
			return -1, nil
		}

		return 1, nil
	case octosql.Bool:
		y, ok := y.(octosql.Bool)
		if !ok {
			return 0, errors.Errorf("type mismatch between values")
		}

		if x == y {
			return 0, nil
		} else if !x && y {
			return -1, nil
		}

		return 1, nil
	default:
		return 0, errors.Errorf("unsupported type in sorting")
	}
}

func createOrderedStream(ob []OrderField, sourceStream RecordStream) (RecordStream, error) {

	records := make([]*Record, 0)

	for {
		rec, err := sourceStream.Next()
		if err == ErrEndOfStream {
			break
		} else if err != nil {
			return nil, errors.Wrap(err, "couldn't get all records")
		}

		records = append(records, rec)
	}

	err := validateRecords(records, ob)
	if err != nil {
		return nil, errors.Wrap(err, "records can't be sorted according to given columns")
	}

	var sortErr error = nil

	sort.Slice(records, func(i, j int) bool {
		iRec := records[i]
		jRec := records[j]

		for _, column := range ob {
			x := iRec.Value(column.ColumnName)
			y := jRec.Value(column.ColumnName)

			if !isSorteable(x) {
				sortErr = errors.Errorf("value %v of type %v is not comparable", x, reflect.TypeOf(x).String())
				return false
			}

			if !isSorteable(x) {
				sortErr = errors.Errorf("value %v of type %v is not comparable", y, reflect.TypeOf(y).String())
				return false
			}

			cmp, err := compare(x, y)
			if err != nil {
				sortErr = errors.Errorf("failed to compare values %v and %v", x, y)
				return false
			}

			answer := false

			if cmp == 0 {
				continue
			} else if cmp > 0 {
				answer = true
			}

			if column.Direction == Ascending {
				answer = !answer
			}

			return answer
		}

		return false
	})

	if sortErr != nil {
		return nil, errors.Wrap(sortErr, "got an error while sorting records")
	}

	return NewInMemoryStream(records), nil
}

func (ob *OrderBy) Get(variables octosql.Variables) (RecordStream, error) {
	sourceStream, err := ob.Source.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get underlying stream in order by")
	}

	orderedStream, err := createOrderedStream(ob.Fields, sourceStream)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create ordered stream from source stream")
	}

	return orderedStream, nil
}
