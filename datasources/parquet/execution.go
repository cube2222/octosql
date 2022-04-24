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

// type Reconstructor struct {
// 	Nullable *struct {
// 		DefinitionLevel int8
// 		Reconstructor   Reconstructor
// 	}
// 	Repeated *struct {
// 		RepetitionLevel int8
// 		Reconstructor   Reconstructor
// 	}
// 	Group           []Reconstructor
// 	LeafColumnIndex *int
// }

// func (*Reconstructor) ReconstructValue() octosql.Value {
// 	// Czyli np. ten Repeated musi mieć dostęp do repetition leveli pod spodem każdej z kolumn, żeby zdecydować, czy jest sens coś próbować brać?
// 	// Lepiej. Z repetition leveli można odszyfrować po ile powinno być branych wartości dla każdego z repeated elementów.
// 	// Czyli patrząc na listy repetition leveli pod spodem (powiedzmy że jesteśmy na poziomie jeden), liczymy maksymalną liczbę jedynek do następnego zera.
// 	// Więc nam mówi ile razy grupę pod spodem (albo cokolwiek pod spodem) odpytujemy "daj mi jeszcze jeden".
// 	// Trochę szkoda życia na to chyba.
// 	// Może przekopiuj po prostu tego całego reconstructa z segmentio i przerób go? Oni to przekminili na pewno dobrze.
// }

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
	_, reconstruct := reconstructFuncOf(0, pf.Schema())

	var row parquet.Row
	pr := parquet.NewReader(pf)
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
	// boolsBuf := make([]bool, 1024)

	// rowGroups := pf.NumRowGroups()
	// for i := 0; i < rowGroups; i++ {
	// 	rg := pf.RowGroup(i)
	// 	rg.Rows()
	// 	octoColumns := make([][]octosql.Value, rg.NumColumns())
	// 	octoColumnIndices := make([]int, rg.NumColumns())
	// 	repetitionLevels := make([][]int8, rg.NumColumns())
	// 	repetitionLevelIndices := make([]int, rg.NumColumns())
	// 	definitionLevels := make([][]int8, rg.NumColumns())
	// 	definitionLevelIndices := make([]int, rg.NumColumns())
	//
	// 	for j := 0; j < rg.NumColumns(); j++ { // Use selected column indices only.
	// 		col := rg.Column(j)
	// 		pages := col.Pages()
	// 		for {
	// 			page, err := pages.ReadPage()
	// 			if err == io.EOF {
	// 				break
	// 			} else if err != nil {
	// 				return fmt.Errorf("couldn't read page: %w", err)
	// 			}
	// 			bufferedPage := page.Buffer()
	//
	// 			repetitionLevels = append(repetitionLevels, bufferedPage.RepetitionLevels())
	// 			definitionLevels = append(definitionLevels, bufferedPage.RepetitionLevels())
	//
	// 			switch typedPage := bufferedPage.(type) {
	// 			case parquet.BooleanReader:
	// 				eofReached := false
	// 				for !eofReached {
	// 					n, err := typedPage.ReadBooleans(boolsBuf)
	// 					if err != nil {
	// 						if err == io.EOF {
	// 							eofReached = true
	// 						} else {
	// 							return fmt.Errorf("couldn't read booleans: %w", err)
	// 						}
	// 					}
	// 					for i := 0; i < n; i++ {
	// 						octoColumns[j] = append(octoColumns[j], octosql.NewBoolean(boolsBuf[i]))
	// 					}
	// 				}
	//
	// 			case parquet.Int32Reader:
	// 			case parquet.Int64Reader:
	// 			case parquet.FloatReader:
	// 			case parquet.DoubleReader:
	// 			case parquet.ByteArrayReader:
	// 			case parquet.FixedLenByteArrayReader:
	// 			}
	//
	// 		}
	//
	// 		if err != nil {
	// 			return fmt.Errorf("couldn't read page: %w", err)
	// 		}
	// 	}
	// }
	return nil
}
