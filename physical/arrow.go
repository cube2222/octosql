package physical

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/scalar"
	"github.com/cube2222/octosql/arrowexec/execution"
	"github.com/cube2222/octosql/arrowexec/nodes"
	"github.com/cube2222/octosql/octosql"
)

type ArrowDatasourceImplementation interface {
	MaterializeArrow(ctx context.Context, env Environment, schema *arrow.Schema, pushedDownPredicates []Expression) (execution.Node, error)
}

// TODO: Here we'll want some code to turn a physical plan into an execution plan.
//       We'll need a plan transformer, but also e.g. translate octosql schemas into arrow schemas.

func (node *Node) MaterializeArrow(ctx context.Context, env Environment) (*execution.NodeWithMeta, error) {
	switch node.NodeType {
	// case NodeTypeMap:
	// 	source, err := MaterializeArrow(ctx, node.Map.Source, env)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("couldn't materialize source: %w", err)
	// 	}
	// 	exprs := make([]execution.Expression, len(node.Map.Expressions))
	// 	for i, expr := range node.Map.Expressions {
	// 		materializedExpr, err := MaterializeArrow(ctx, expr, env.WithRecordSchema(node.Map.Source.Schema))
	// 		if err != nil {
	// 			return nil, fmt.Errorf("couldn't materialize expression: %w", err)
	// 		}
	// 		exprs[i] = materializedExpr
	// 	}
	// 	return &Map{
	// 		Source: source,
	// 		Exprs:  exprs,
	// 		Schema: node.Schema,
	// 	}, nil
	// case NodeTypeTableValuedFunction:
	// 	switch node.TableValuedFunction.Name {
	// 	case "range":
	// 		// TODO: Support expressions as arguments of the range node.
	// 		return &Range{
	// 			LocalName: node.Schema.Fields[0].Name,
	// 			Start:     int32(node.TableValuedFunction.Arguments["start"].Expression.Expression.Constant.Value.Int),
	// 			End:       int32(node.TableValuedFunction.Arguments["end"].Expression.Expression.Constant.Value.Int),
	// 		}, nil
	// 	default:
	// 		panic("invalid table valued function")
	// 	}
	// case NodeTypeInMemoryRecords:
	// 	return &InMemoryRecords{
	// 		Records: node.InMemoryRecords.Records,
	// 		Schema:  node.Schema,
	// 	}, nil
	case NodeTypeDatasource:
		uniqueToColname := make(map[string]string)
		for k, v := range node.Datasource.VariableMapping {
			uniqueToColname[v] = strings.TrimPrefix(k, node.Datasource.Alias+".")
		}
		predicatesOriginalNames := renameExpressionSliceRecordVariables(uniqueToColname, node.Datasource.Predicates)

		fieldsOriginalNames := make([]SchemaField, len(node.Schema.Fields))
		for i := range node.Schema.Fields {
			fieldsOriginalNames[i] = SchemaField{
				Name: uniqueToColname[node.Schema.Fields[i].Name],
				Type: node.Schema.Fields[i].Type,
			}
		}
		schemaOriginalNames := NewSchema(fieldsOriginalNames, node.Schema.TimeField)

		var out execution.Node
		if impl, ok := node.Datasource.DatasourceImplementation.(ArrowDatasourceImplementation); ok {
			var err error
			out, err = impl.MaterializeArrow(ctx, env, OctoSQLToArrowSchema(schemaOriginalNames), predicatesOriginalNames)
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize arrow datasource: %w", err)
			}
		}
		return &execution.NodeWithMeta{
			Node:   out,
			Schema: OctoSQLToArrowSchema(node.Schema),
		}, nil

		panic("datasource not implemented for arrow")

		// TODO: By default materialize as normal and provide compatibility layer.
	// case NodeTypeDatasource:
	// 	uniqueToColname := make(map[string]string)
	// 	for k, v := range node.Datasource.VariableMapping {
	// 		uniqueToColname[v] = strings.TrimPrefix(k, node.Datasource.Alias+".")
	// 	}
	// 	predicatesOriginalNames := RenameExpressionSliceRecordVariables(uniqueToColname, node.Datasource.Predicates)
	//
	// 	fieldsOriginalNames := make([]SchemaField, len(node.Schema.Fields))
	// 	for i := range node.Schema.Fields {
	// 		fieldsOriginalNames[i] = SchemaField{
	// 			Name: uniqueToColname[node.Schema.Fields[i].Name],
	// 			Type: node.Schema.Fields[i].Type,
	// 		}
	// 	}
	// 	schemaOriginalNames := NewSchema(fieldsOriginalNames, node.Schema.TimeField)
	//
	// 	materialized, err := node.Datasource.DatasourceImplementation.Materialize(ctx, env, schemaOriginalNames, predicatesOriginalNames)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("couldn't materialize datasource: %w", err)
	// 	}
	//
	// 	return &Datasource{
	// 		Schema: node.Schema,
	// 		Source: materialized,
	// 	}, nil
	case NodeTypeFilter:
		source, err := node.Filter.Source.MaterializeArrow(ctx, env)
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize filter source: %w", err)
		}
		predicate, err := node.Filter.Predicate.MaterializeArrow(ctx, env, OctoSQLToArrowSchema(node.Filter.Source.Schema))
		if err != nil {
			return nil, fmt.Errorf("couldn't materialize filter predicate: %w", err)
		}
		return &execution.NodeWithMeta{
			Node:   nodes.NewFilter(source, predicate),
			Schema: OctoSQLToArrowSchema(node.Schema),
		}, nil
	default:
		panic(fmt.Sprintf("invalid node type: %s", node.NodeType))
	}
}

func (expr *Expression) MaterializeArrow(ctx context.Context, env Environment, recordSchema *arrow.Schema) (execution.Expression, error) {
	switch expr.ExpressionType {
	case ExpressionTypeConstant:
		return &execution.Constant{
			Value: OctoSQLValueToArrowScalar(expr.Constant.Value),
		}, nil
	case ExpressionTypeVariable:
		switch expr.Variable.IsLevel0 {
		case true:
			for i, field := range recordSchema.Fields() {
				if field.Name == expr.Variable.Name {
					return execution.NewRecordVariable(i), nil
				}
			}
			panic("unreachable")
		default:
			panic("implement me")
		}
	case ExpressionTypeFunctionCall:
		args := make([]execution.Expression, len(expr.FunctionCall.Arguments))
		for i := range expr.FunctionCall.Arguments {
			arg, err := expr.FunctionCall.Arguments[i].MaterializeArrow(ctx, env, recordSchema)
			if err != nil {
				return nil, fmt.Errorf("couldn't materialize argument: %w", err)
			}
			args[i] = arg
		}

		switch expr.FunctionCall.Name {
		case "=":
			// TODO: Support equality of other types.
			return &execution.ArrowComputeFunctionCall{
				Name:      "equal",
				Arguments: args,
			}, nil
			// default:
		// args := make([]Expression, len(expr.FunctionCall.Arguments))
		// for i := range expr.FunctionCall.Arguments {
		// 	arg, err := MaterializeArrow(ctx, expr.FunctionCall.Arguments[i], env)
		// 	if err != nil {
		// 		return nil, fmt.Errorf("couldn't materialize argument: %w", err)
		// 	}
		// 	args[i] = arg
		// }
		// return &CallBuiltinFunc{Name: expr.FunctionCall.Name, Arguments: args}, nil
		default:
			panic("implement me")
		}
	default:
		panic(fmt.Sprintf("invalid expression type: %s", expr.ExpressionType))
	}
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
