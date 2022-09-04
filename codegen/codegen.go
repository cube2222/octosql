package codegen

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type Value struct {
	Type      octosql.Type
	Reference string
	// TODO: Alternatives for union types
}

type Record struct {
	Fields []RecordField
	// TODO: Add retractions. Their handling should only be generated if they are statically possible.
}

type RecordField struct {
	Name  string
	Value Value
}

func (r *Record) GetValue(name string) (*Value, bool) {
	for i := range r.Fields {
		if r.Fields[i].Name == name {
			return &r.Fields[i].Value, true
		}
	}
	return nil, false
}

type Context struct {
	VariableContext *VariableContext
}

func (ctx Context) WithRecord(record Record) Context {
	newCtx := ctx
	newCtx.VariableContext = newCtx.VariableContext.WithRecord(record)
	return newCtx
}

type VariableContext struct {
	Parent *VariableContext
	Record Record
}

func (varCtx *VariableContext) WithRecord(record Record) *VariableContext {
	return &VariableContext{
		Parent: varCtx,
		Record: record,
	}
}

func (varCtx *VariableContext) GetValue(name string) *Value {
	cur := varCtx
	for cur != nil {
		out, ok := cur.Record.GetValue(name)
		if ok {
			return out
		}
	}
	panic("variable not found")
}

func Generate(node physical.Node) string {
	g := CodeGenerator{
		body:                &bytes.Buffer{},
		imports:             map[string]bool{},
		uniqueVariableNames: map[string]int{},
	}
	ctx := Context{}

	g.Node(ctx, node, func(record Record) {
		var references []string
		for i := range record.Fields {
			references = append(references, record.Fields[i].Value.Reference)
		}
		g.Printfln("fmt.Println(%s)\n", strings.Join(references, ", "))
	})

	var fullOutput bytes.Buffer
	fmt.Fprintf(&fullOutput, "package main\n")

	{
		// TODO: This should really iterate over available aggregate descriptors.
	}

	fmt.Fprintf(&fullOutput, `import "fmt"`+"\n")
	fmt.Fprintf(&fullOutput, `import "github.com/cube2222/octosql/codegen/lib"`+"\n")
	fmt.Fprintf(&fullOutput, `const noUnusedMark = lib.NoUnusedMark`+"\n")
	fmt.Fprintf(&fullOutput, "func main() {\n")
	fmt.Fprint(&fullOutput, g.body.String())
	fmt.Fprintf(&fullOutput, "\n")
	fmt.Fprintf(&fullOutput, "}\n")

	return fullOutput.String()
}

type CodeGenerator struct {
	body                *bytes.Buffer
	imports             map[string]bool
	uniqueVariableNames map[string]int
}

func (g *CodeGenerator) Printfln(format string, args ...interface{}) {
	fmt.Fprintf(g.body, format+"\n", args...)
}

func (g *CodeGenerator) Unique(name string) string {
	g.uniqueVariableNames[name]++
	return fmt.Sprintf("%s%d", name, g.uniqueVariableNames[name])
}

func GoType(t octosql.Type) string {
	switch t.TypeID {
	case octosql.TypeIDInt:
		return "int"
	default:
		panic("implement me")
	}
}
