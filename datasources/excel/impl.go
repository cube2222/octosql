package excel

import (
	"context"
	"fmt"
	"strconv"

	"github.com/davecgh/go-spew/spew"
	"github.com/xuri/excelize/v2"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
)

func Creator() func(name string, options map[string]string) (physical.DatasourceImplementation, physical.Schema, error) {
	return func(name string, options map[string]string) (physical.DatasourceImplementation, physical.Schema, error) {
		rootCell := "A1"
		if customRootCell, ok := options["root_cell"]; ok {
			rootCell = customRootCell
		}
		rootCol, rootRow, err := excelize.CellNameToCoordinates(rootCell)
		if err != nil {
			return nil, physical.Schema{}, fmt.Errorf("couldn't parse root cell: %w", err)
		}

		header := true
		if customHeader, ok := options["header"]; ok {
			header, err = strconv.ParseBool(customHeader)
			if err != nil {
				return nil, physical.Schema{}, fmt.Errorf("couldn't parse header option, must be true or false: %w", err)
			}
		}
		header = header

		sheet := "Sheet1"
		if customSheet, ok := options["sheet"]; ok {
			sheet = customSheet
		}

		f, err := excelize.OpenFile(name)
		if err != nil {
			return nil, physical.Schema{}, fmt.Errorf("couldn't open excel file: %w", err)
		}

		tableWidth := 1

		for {
			curCellName, err := excelize.CoordinatesToCellName(rootCol+tableWidth, rootRow)
			if err != nil {
				panic(err) // TODO: Fixme
			}
			value, err := f.GetCellValue(sheet, curCellName)
			if err != nil {
				panic(err) // TODO: Fixme
			}
			if value == "" {
				break
			}
			tableWidth++
		}

		v, err := f.GetCellValue(sheet, "A2")
		spew.Dump(v, err)
		v2, err := f.GetCellValue(sheet, "B2")
		spew.Dump(v2, err)
		v3, err := f.GetCellValue(sheet, "C2", excelize.Options{RawCellValue: true})
		spew.Dump(v3, err)

		s1, err := f.GetCellStyle(sheet, "A2")
		spew.Dump(s1, err)
		s2, err := f.GetCellStyle(sheet, "B2")
		spew.Dump(s2, err)
		s3, err := f.GetCellStyle(sheet, "C2")
		spew.Dump(s3, err)

		t1, err := f.GetCellType(sheet, "A2")
		spew.Dump(t1, err)
		t2, err := f.GetCellType(sheet, "B2")
		spew.Dump(t2, err)
		t3, err := f.GetCellType(sheet, "C2")
		spew.Dump(t3, err)

		// If a cell type is CellTypeDate then we treat it as a time value.
		// Basically, if it has a sensible CellType, we use that.
		// Otherwise, if it's "general" (unset), then we try to infer like in the CSV datasource.

		spew.Dump(f.Styles.NumFmts)

		panic(tableWidth)

		panic("implement me")

		// TODO: Interpret empty field value as NULL.
	}
}

type impl struct {
	path           string
	header         bool
	separator      rune
	fileFieldNames []string
}

func (i *impl) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (execution.Node, error) {
	return &DatasourceExecuting{
		path:           i.path,
		fields:         schema.Fields,
		header:         i.header,
		separator:      i.separator,
		fileFieldNames: i.fileFieldNames,
	}, nil
}

func (i *impl) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}
