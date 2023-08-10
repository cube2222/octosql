package physical

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/scalar"
	"github.com/cube2222/octosql/arrowexec/execution"
	"github.com/cube2222/octosql/octosql"
)

type ArrowDatasourceImplementation interface {
	MaterializeArrow(ctx context.Context, env Environment, schema *arrow.Schema, pushedDownPredicates []Expression) (execution.Node, error)
}

func OctoSQLToArrowSchema(schema Schema) *arrow.Schema {
	fields := make([]arrow.Field, len(schema.Fields))
	for i, field := range schema.Fields {
		fields[i] = arrow.Field{
			Name:     field.Name,
			Nullable: OctoSQLTypeIsNullable(field.Type),
			Type:     OctoSQLToArrowType(field.Type),
		}
	}
	return arrow.NewSchema(fields, nil)
}

func OctoSQLTypeIsNullable(t octosql.Type) bool {
	return octosql.Null.Is(t) == octosql.TypeRelationIs
}

func OctoSQLToArrowType(t octosql.Type) arrow.DataType {
	switch t.TypeID {
	case octosql.TypeIDNull:
		return arrow.Null
	case octosql.TypeIDInt:
		return arrow.PrimitiveTypes.Int64
	case octosql.TypeIDFloat:
		return arrow.PrimitiveTypes.Float64
	case octosql.TypeIDBoolean:
		return arrow.FixedWidthTypes.Boolean
	case octosql.TypeIDString:
		return arrow.BinaryTypes.String // TODO: LargeString
	case octosql.TypeIDTime:
		return arrow.FixedWidthTypes.Timestamp_ns
	case octosql.TypeIDDuration:
		return arrow.FixedWidthTypes.Duration_ns
	case octosql.TypeIDList:
		return arrow.ListOf(OctoSQLToArrowType(*t.List.Element))
	case octosql.TypeIDStruct:
		fields := make([]arrow.Field, len(t.Struct.Fields))
		for i, field := range t.Struct.Fields {
			fields[i] = arrow.Field{
				Name:     field.Name,
				Nullable: OctoSQLTypeIsNullable(field.Type),
				Type:     OctoSQLToArrowType(field.Type),
			}
		}
		return arrow.StructOf(fields...)
	case octosql.TypeIDTuple:
		fields := make([]arrow.Field, len(t.Tuple.Elements))
		for i, element := range t.Tuple.Elements {
			fields[i] = arrow.Field{
				Name:     fmt.Sprintf("_%d", i),
				Nullable: OctoSQLTypeIsNullable(element),
				Type:     OctoSQLToArrowType(element),
			}
		}
		return arrow.StructOf(fields...)
	case octosql.TypeIDUnion:
		// TODO: If there are just two options and one is null, then it's just a nullable field.
		//		 Otherwise, it's a union, and we keep the null type?
		if len(t.Union.Alternatives) == 2 {
			if t.Union.Alternatives[0].TypeID == octosql.TypeIDNull {
				return OctoSQLToArrowType(t.Union.Alternatives[1])
			}
			if t.Union.Alternatives[1].TypeID == octosql.TypeIDNull {
				return OctoSQLToArrowType(t.Union.Alternatives[0])
			}
		}

		alternatives := t.Union.Alternatives
		arrowTypes := make([]arrow.Field, 0, len(alternatives))
		arrowTypeCodes := make([]arrow.UnionTypeCode, 0, len(alternatives))
		for i, alternative := range alternatives {
			arrowTypes = append(arrowTypes, arrow.Field{
				Name:     fmt.Sprint(i),
				Type:     OctoSQLToArrowType(alternative),
				Nullable: false,
			})
			arrowTypeCodes = append(arrowTypeCodes, arrow.UnionTypeCode(i))
		}

		return arrow.UnionOf(arrow.SparseMode, arrowTypes, arrowTypeCodes)
	case octosql.TypeIDAny:
		panic("any type is not supported in arrow")
	default:
		panic(fmt.Errorf("invalid type: %v", t))
	}
}

func OctoSQLValueToArrowScalar(v octosql.Value) scalar.Scalar {
	switch v.TypeID {
	case octosql.TypeIDNull:
		return &scalar.Null{}
	case octosql.TypeIDInt:
		return scalar.NewInt64Scalar(int64(v.Int))
	case octosql.TypeIDFloat:
		return scalar.NewFloat64Scalar(v.Float)
	case octosql.TypeIDBoolean:
		return scalar.NewBooleanScalar(v.Boolean)
	case octosql.TypeIDString:
		return scalar.NewStringScalar(v.Str)
	case octosql.TypeIDTime:
		return scalar.NewTime64Scalar(arrow.Time64(v.Time.UnixNano()), arrow.FixedWidthTypes.Time64ns)
	case octosql.TypeIDDuration:
		return scalar.NewDurationScalar(arrow.Duration(v.Duration), arrow.FixedWidthTypes.Duration_ns)
	case octosql.TypeIDList:
		// TODO: Fixme
		panic("not implemented")
	case octosql.TypeIDStruct:
		fields := make([]scalar.Scalar, len(v.Struct))
		for i := range v.Struct {
			fields[i] = OctoSQLValueToArrowScalar(v.Struct[i])
		}

		return scalar.NewStructScalar(fields, OctoSQLToArrowType(v.Type()))
	case octosql.TypeIDTuple:
		fields := make([]scalar.Scalar, len(v.Tuple))
		for i := range v.Tuple {
			fields[i] = OctoSQLValueToArrowScalar(v.Tuple[i])
		}

		return scalar.NewStructScalar(fields, OctoSQLToArrowType(v.Type()))
	default:
		panic(fmt.Sprintf("faled to turn '%s' into arrow scalar", v.Type()))
	}
}
