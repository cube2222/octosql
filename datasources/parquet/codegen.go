package parquet

import (
	"fmt"
	"strings"

	"github.com/cube2222/octosql/codegen"
	"github.com/cube2222/octosql/physical"
)

func (i *impl) Generate(g *codegen.CodeGenerator, ctx codegen.Context, schema physical.Schema, produce func(reference codegen.Record)) {
	g.AddImport("github.com/segmentio/parquet-go")
	g.AddImport("os")
	g.AddImport("io")

	file := g.Unique("file")
	stat := g.Unique("stat")
	pf := g.Unique("pf")
	pr := g.Unique("pr")
	row := g.Unique("row")

	usedFields := make([]string, len(schema.Fields))
	for i := range usedFields {
		usedFields[i] = schema.Fields[i].Name
	}

	usedFieldsLiterals := make([]string, len(usedFields))
	for i := range usedFieldsLiterals {
		usedFieldsLiterals[i] = `"` + usedFields[i] + `"`
	}

	mapping := map[string]string{
		"file":       file,
		"stat":       stat,
		"path":       i.path,
		"pf":         pf,
		"pr":         pr,
		"row":        row,
		"usedFields": fmt.Sprintf("[]string{%s}", strings.Join(usedFieldsLiterals, ", ")),
	}

	// TODO: Fix context.Background()
	g.PrintflnAdvanced(`
$file, err := os.Open("$path")
if err != nil {
	panic("couldn't open file")
}
defer $file.Close()
$stat, err := $file.Stat()
if err != nil {
	panic("couldn't stat file")
}

$pf, err := parquet.OpenFile($file, $stat.Size(), &parquet.FileConfig{
	SkipPageIndex:    true,
	SkipBloomFilters: true,
})
if err != nil {
	panic("couldn't open file")	
}
$pf.Schema().MakeColumnReadRowFunc($usedFields)
`, mapping)

	g.PrintflnAdvanced(`
var $row parquet.Row
$pr := parquet.NewReader($pf)
for {
	$row, err := $pr.ReadRow($row)
	if err != nil {
		if err == io.EOF {
			break
		}
		panic("couldn't read row")
	}

`, mapping)

	recordFields := codegenReconstructFuncOfSchemaFields(g, row, schema, i.schema, usedFields)

	produce(codegen.Record{
		Fields: recordFields,
	})

	g.Printfln("}")
}
