package csv

// only comma-separated now. Changing that means setting RecordStream.r.Coma
// .csv reader trims leading white space(s)

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"unicode"

	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/config"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
)

var availableFilters = map[physical.FieldType]map[physical.Relation]struct{}{
	physical.Primary:   make(map[physical.Relation]struct{}),
	physical.Secondary: make(map[physical.Relation]struct{}),
}

type DataSource struct {
	excelFile       *excelize.File
	sheet           string
	rootCoordinates string
	alias           string
}

func getCellRowCol(cell string) (row, col string) {
	for index, rune := range cell {
		if unicode.IsDigit(rune) {
			return cell[index:], cell[:index]
		}
	}

	return "", cell
}

func getRowColCoords(cell string) (row int, col int, err error) {
	rowStr, colStr := getCellRowCol(cell)
	col = excelize.TitleToNumber(colStr)
	rowTmp, err := strconv.ParseInt(rowStr, 10, 64)
	if err != nil {
		return 0, 0, errors.Wrap(err, "couldn't parse row")
	}

	return int(rowTmp) - 1, col, nil
}

func NewDataSourceBuilderFactory() physical.DataSourceBuilderFactory {
	return physical.NewDataSourceBuilderFactory(
		func(ctx context.Context, matCtx *physical.MaterializationContext, dbConfig map[string]interface{}, filter physical.Formula, alias string) (execution.Node, error) {
			path, err := config.GetString(dbConfig, "path")
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get path")
			}
			sheet, err := config.GetString(dbConfig, "sheet", config.WithDefault(""))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get sheet")
			}
			rootCoordinates, err := config.GetString(dbConfig, "rootCoordinates", config.WithDefault("A1"))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get table root coordinates")
			}

			excelFile, err := excelize.OpenFile(path)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't open excel file")
			}

			if len(sheet) == 0 {
				minIndex := math.MaxInt64
				sheets := excelFile.GetSheetMap()
				for i := range sheets {
					if i < minIndex {
						minIndex = i
					}
				}

				if minIndex == math.MaxInt64 {
					return nil, fmt.Errorf("no sheets found")
				}
				sheet = sheets[minIndex]
			}

			row, col, err := getRowColCoords(rootCoordinates)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get table root coordinates")
			}

			rows, err := excelFile.Rows(sheet)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get table rows")
			}
			for i := 0; rows.Next() && i != row; i++ {
			}

			var width int
			var v string
			for i := 0; col+i < len(rows.Columns()); i++ {
				if len(v) == 0 {
					break
				}
			}

			return &DataSource{
				excelFile:       excelFile,
				sheet:           sheet,
				rootCoordinates: rootCoordinates,
				alias:           alias,
			}, nil
		},
		nil,
		availableFilters,
	)
}

// NewDataSourceBuilderFactoryFromConfig creates a data source builder factory using the configuration.
func NewDataSourceBuilderFactoryFromConfig(dbConfig map[string]interface{}) (physical.DataSourceBuilderFactory, error) {
	return NewDataSourceBuilderFactory(), nil
}

func (ds *DataSource) Get(variables octosql.Variables) (execution.RecordStream, error) {
	excelFile, err := excelize.OpenFile(ds.path)
	if err != nil {
		log.Fatal(err)
	}

	excelFile.GetRows("Sheet1")

	columns, err := r.Read()
	if err != nil {
		return nil, errors.Wrap(err, "couldn't read column names")
	}

	aliasedFields := make([]octosql.VariableName, 0)
	for _, c := range columns {
		aliasedFields = append(aliasedFields, octosql.VariableName(fmt.Sprintf("%s.%s", ds.alias, c)))
	}
	r.FieldsPerRecord = len(aliasedFields)

	set := make(map[octosql.VariableName]struct{})
	for _, f := range aliasedFields {
		if _, present := set[f]; present {
			return nil, errors.New("column names not unique")
		}
		set[f] = struct{}{}
	}

	return &RecordStream{
		file:          file,
		r:             r,
		isDone:        false,
		alias:         ds.alias,
		aliasedFields: aliasedFields,
	}, nil
}

type RecordStream struct {
	file          *os.File
	r             *csv.Reader
	isDone        bool
	alias         string
	aliasedFields []octosql.VariableName
}

func (rs *RecordStream) Close() error {
	err := rs.file.Close()
	if err != nil {
		return errors.Wrap(err, "Couldn't close underlying file")
	}

	return nil
}

func (rs *RecordStream) Next() (*execution.Record, error) {
	if rs.isDone {
		return nil, execution.ErrEndOfStream
	}

	line, err := rs.r.Read()
	if err == io.EOF {
		rs.isDone = true
		rs.file.Close()
		return nil, execution.ErrEndOfStream
	}

	if err != nil {
		return nil, errors.Wrap(err, "couldn't read record")
	}

	aliasedRecord := make(map[octosql.VariableName]octosql.Value)
	for i, v := range line {
		aliasedRecord[rs.aliasedFields[i]] = execution.ParseType(v)
	}

	return execution.NewRecord(rs.aliasedFields, aliasedRecord), nil
}
