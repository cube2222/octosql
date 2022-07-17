package formats

import (
	"fmt"
	"io"
	"log"
	"time"

	"github.com/valyala/fastjson"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type JSONFormatter struct {
	buf    []byte
	arena  *fastjson.Arena
	w      io.Writer
	fields []physical.SchemaField
}

func NewJSONFormatter(w io.Writer) *JSONFormatter {
	return &JSONFormatter{
		buf:   make([]byte, 0, 1024),
		arena: new(fastjson.Arena),
		w:     w,
	}
}

func (t *JSONFormatter) SetSchema(schema physical.Schema) {
	t.fields = withoutQualifiers(schema.Fields)
}

func (t *JSONFormatter) Write(values []octosql.Value) error {
	obj := t.arena.NewObject()
	for i := range t.fields {
		obj.Set(t.fields[i].Name, ValueToJson(t.arena, t.fields[i].Type, values[i]))
	}

	t.buf = obj.MarshalTo(t.buf)
	t.buf = append(t.buf, '\n')
	t.w.Write(t.buf)
	t.buf = t.buf[:0]
	t.arena.Reset()
	return nil
}

func ValueToJson(arena *fastjson.Arena, t octosql.Type, value octosql.Value) *fastjson.Value {
	if t.TypeID == octosql.TypeIDUnion {
		for i := range t.Union.Alternatives {
			if t.Union.Alternatives[i].TypeID == value.TypeID {
				return ValueToJson(arena, t.Union.Alternatives[i], value)
			}
		}
		log.Printf("Invalid value of type '%s' for union type '%s'. Using null.", value.TypeID.String(), t.String())
		return arena.NewNull()
	}

	switch value.TypeID {
	case octosql.TypeIDNull:
		return arena.NewNull()
	case octosql.TypeIDInt:
		return arena.NewNumberInt(value.Int)
	case octosql.TypeIDFloat:
		return arena.NewNumberFloat64(value.Float)
	case octosql.TypeIDBoolean:
		if value.Boolean {
			return arena.NewTrue()
		} else {
			return arena.NewFalse()
		}
	case octosql.TypeIDString:
		return arena.NewString(value.Str)
	case octosql.TypeIDTime:
		return arena.NewString(value.Time.Format(time.RFC3339))
	case octosql.TypeIDDuration:
		return arena.NewString(value.Duration.String())
	case octosql.TypeIDList:
		arr := arena.NewArray()
		for i := range value.List {
			arr.SetArrayItem(i, ValueToJson(arena, *t.List.Element, value.List[i]))
		}
		return arr
	case octosql.TypeIDStruct:
		arr := arena.NewObject()
		for i := range value.Struct {
			arr.Set(t.Struct.Fields[i].Name, ValueToJson(arena, t.Struct.Fields[i].Type, value.Struct[i]))
		}
		return arr
	case octosql.TypeIDTuple:
		arr := arena.NewArray()
		for i := range value.Tuple {
			arr.SetArrayItem(i, ValueToJson(arena, t.Tuple.Elements[i], value.Tuple[i]))
		}
		return arr
	default:
		panic(fmt.Sprintf("invalid octosql value type to print: %s", value.TypeID.String()))
	}
}

func (t *JSONFormatter) Close() error {
	return nil
}
