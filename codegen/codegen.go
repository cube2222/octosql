package codegen

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type Register interface {
	TypeID() string
	AsType(typeID octosql.TypeID) string
	DebugRawReference() string
	SetTypeIDCommand(typeID octosql.TypeID) string
}

type ConstTypeValue struct {
	// TODO: Is there any performance benefit in this specialization?
	typeID    octosql.TypeID
	reference string
}

func (c *ConstTypeValue) TypeID() string {
	return fmt.Sprintf("octosql.TypeID(%d)", int(c.typeID))
}

func (c *ConstTypeValue) AsType(t octosql.TypeID) string {
	if t != c.typeID {
		panic("type mismatch")
	}
	return c.reference
}

func (c *ConstTypeValue) DebugRawReference() string {
	return c.reference
}

func (c *ConstTypeValue) SetTypeIDCommand(typeID octosql.TypeID) string {
	return ""
}

type UnionTypeValue struct {
	objectReference string
}

func (u *UnionTypeValue) TypeID() string {
	return fmt.Sprintf("%s.TypeID", u.objectReference)
}

func (c *UnionTypeValue) AsType(t octosql.TypeID) string {
	return c.objectReference + "." + TypeIDFieldName(t)
}

func (u *UnionTypeValue) DebugRawReference() string {
	return u.objectReference
}

func (c *UnionTypeValue) SetTypeIDCommand(typeID octosql.TypeID) string {
	return fmt.Sprintf("%s = octosql.TypeID(%d)", c.TypeID(), int(typeID))
}

type Record struct {
	Fields []RecordField
	// TODO: Add retractions. Their handling should only be generated if they are statically possible.
}

type RecordField struct {
	Name  string
	Value Register
}

func (r *Record) GetValue(name string) (*Register, bool) {
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

func (varCtx *VariableContext) GetValue(name string) *Register {
	cur := varCtx
	for cur != nil {
		out, ok := cur.Record.GetValue(name)
		if ok {
			return out
		}
		cur = cur.Parent
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
			references = append(references, record.Fields[i].Value.DebugRawReference())
		}
		g.Printfln("fmt.Println(%s)\n", strings.Join(references, ", "))
	})

	var fullOutput bytes.Buffer
	fmt.Fprintf(&fullOutput, "package main\n")

	fmt.Fprintf(&fullOutput, `import "fmt"`+"\n")
	fmt.Fprintf(&fullOutput, `import "github.com/cube2222/octosql/codegen/lib"`+"\n")
	fmt.Fprintf(&fullOutput, `import "github.com/cube2222/octosql/octosql"`+"\n")
	for i := range g.imports {
		fmt.Fprintf(&fullOutput, `import "%s"`+"\n", i)
	}
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

func (g *CodeGenerator) PrintflnAdvanced(format string, vars map[string]string) {
	var params []string
	for k, v := range vars {
		params = append(params, "$"+k, v)
	}

	fmt.Fprint(g.body, strings.NewReplacer(params...).Replace(format))
}

func (g *CodeGenerator) Unique(name string) string {
	g.uniqueVariableNames[name]++
	return fmt.Sprintf("%s%d", name, g.uniqueVariableNames[name])
}

func (g *CodeGenerator) AddImport(name string) {
	g.imports[name] = true
}

func (g *CodeGenerator) DeclareVariable(name string, t octosql.Type) Register {
	uniqueName := g.Unique(name)
	g.Printfln("var %s %s; _ = %s", uniqueName, GoType(t), uniqueName)
	if t.TypeID == octosql.TypeIDUnion {
		return &UnionTypeValue{
			objectReference: uniqueName,
		}
	} else {
		return &ConstTypeValue{
			typeID:    t.TypeID,
			reference: uniqueName,
		}
	}
}

type GenerateableDataSource interface {
	Generate(g *CodeGenerator, ctx Context, schema physical.Schema, produce func(reference Record))
}

func GoType(t octosql.Type) string {
	switch t.TypeID {
	case octosql.TypeIDInt:
		return "int"
	case octosql.TypeIDFloat:
		return "float64"
	case octosql.TypeIDString:
		return "string"
	case octosql.TypeIDBoolean:
		return "bool"
	case octosql.TypeIDNull:
		return "struct{}"
	case octosql.TypeIDUnion:
		var buf bytes.Buffer
		buf.WriteString("struct {\n")
		buf.WriteString("TypeID octosql.TypeID\n")
		for _, alternative := range t.Union.Alternatives {
			buf.WriteString(TypeIDFieldName(alternative.TypeID))
			buf.WriteString(" ")
			buf.WriteString(GoType(alternative))
			buf.WriteString("\n")
		}
		buf.WriteString("}")
		return buf.String()
	default:
		panic(fmt.Sprintf("implement me: %s", t.TypeID))
	}
}

func TypeIDFieldName(typeID octosql.TypeID) string {
	switch typeID {
	case octosql.TypeIDInt:
		return "Int"
	case octosql.TypeIDFloat:
		return "Float"
	case octosql.TypeIDString:
		return "String"
	case octosql.TypeIDBoolean:
		return "Boolean"
	case octosql.TypeIDNull:
		return "Null"
	default:
		panic("implement me")
	}
}
