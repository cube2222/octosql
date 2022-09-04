package json

import (
	"github.com/cube2222/octosql/codegen"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

func (i *impl) Generate(g *codegen.CodeGenerator, ctx codegen.Context, schema physical.Schema, produce func(reference codegen.Record)) {
	g.AddImport("github.com/valyala/fastjson")
	g.AddImport("github.com/cube2222/octosql/execution/files")
	g.AddImport("bufio")
	g.AddImport("context")

	file := g.Unique("file")
	scanner := g.Unique("sc")
	parser := g.Unique("p")
	value := g.Unique("v")
	object := g.Unique("o")

	// TODO: Fix context.Background()
	g.Printfln(`%s, err := files.OpenLocalFile(context.Background(), "%s", files.WithTail(%t))`, file, i.path, i.tail)
	g.PrintflnAdvanced(`
if err != nil {
	panic(fmt.Errorf("couldn't open local file: %w", err))
}
defer $file.Close()

$sc := bufio.NewScanner($file)
$sc.Buffer(nil, 1024*1024)
var $p fastjson.Parser
for $sc.Scan() {
	$v, err := $p.ParseBytes($sc.Bytes())
	if err != nil {
        panic(fmt.Errorf("couldn't parse json: %w", err))
	}
	if $v.Type() != fastjson.TypeObject {
		panic(fmt.Errorf("expected JSON object, got '%s'", $sc.Text()))
	}
	$o, err := $v.Object()
	if err != nil {
		panic(fmt.Errorf("expected JSON object, got '%s'", $sc.Text()))
	}
`, map[string]string{
		"file": file,
		"sc":   scanner,
		"p":    parser,
		"v":    value,
		"o":    object,
	})

	recordFields := make([]codegen.RecordField, len(schema.Fields))
	for i := range schema.Fields {
		fieldValueRaw := g.Unique("fieldValueRaw")
		g.Printfln(`%s := %s.Get("%s")`, fieldValueRaw, object, schema.Fields[i].Name)

		fieldValue := g.DeclareVariable("fieldValue", schema.Fields[i].Type)

		g.Printfln("if %s != nil {", fieldValueRaw)

		g.Printfln("switch %s.Type() {", fieldValueRaw)
		if octosql.Float.Is(schema.Fields[i].Type) == octosql.TypeRelationIs {
			g.Printfln("case fastjson.TypeNumber:")
			g.Printfln("%s, _ = %s.Float64()", fieldValue.AsType(octosql.TypeIDFloat), fieldValueRaw)
			g.Printfln(fieldValue.SetTypeIDCommand(octosql.TypeIDFloat))
		}
		if octosql.String.Is(schema.Fields[i].Type) == octosql.TypeRelationIs {
			g.Printfln("case fastjson.TypeString:")
			fieldValueBytes := g.Unique("fieldValueBytes")
			g.Printfln("%s, _ := %s.StringBytes()", fieldValueBytes, fieldValueRaw)
			g.Printfln("%s = string(%s)", fieldValue.AsType(octosql.TypeIDString), fieldValueBytes)
			g.Printfln(fieldValue.SetTypeIDCommand(octosql.TypeIDString))
		}
		g.Printfln("}")

		g.Printfln("}")

		recordFields[i] = codegen.RecordField{
			Name:  schema.Fields[i].Name,
			Value: fieldValue,
		}
	}

	produce(codegen.Record{
		Fields: recordFields,
	})

	g.Printfln("}")
	g.PrintflnAdvanced(`
if err := $sc.Err(); err != nil {
	panic(fmt.Errorf("couldn't read file: %w", err))
}
`, map[string]string{
		"sc": scanner,
	})
}
