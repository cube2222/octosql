package logical

import (
	"context"
	"fmt"
	"time"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type Environment struct {
	CommonTableExpressions map[string]CommonTableExpression
	TableValuedFunctions   map[string]TableValuedFunctionDescription
	UniqueVariableNames    *VariableMapping
	UniqueNameGenerator    map[string]int
}

func (env *Environment) GetUnique(name string) string {
	index := env.UniqueNameGenerator[name]
	env.UniqueNameGenerator[name] = index + 1
	return fmt.Sprintf("%s_%d", name, index)
}

type CommonTableExpression struct {
	Node                  physical.Node
	UniqueVariableMapping map[string]string
}

func (env *Environment) WithRecordUniqueVariableNames(record map[string]string) Environment {
	return Environment{
		CommonTableExpressions: env.CommonTableExpressions,
		TableValuedFunctions:   env.TableValuedFunctions,
		UniqueVariableNames:    env.UniqueVariableNames.WithRecordMapping(record),
		UniqueNameGenerator:    env.UniqueNameGenerator,
	}
}

type VariableMapping struct {
	Parent  *VariableMapping
	Mapping map[string]string
}

func (mapping *VariableMapping) WithRecordMapping(record map[string]string) *VariableMapping {
	return &VariableMapping{
		Parent:  mapping,
		Mapping: record,
	}
}

func (mapping *VariableMapping) GetUniqueName(name string) (string, bool) {
	if mapping == nil {
		return "", false
	}
	unique, ok := GetUniqueNameMatchingVariable(mapping.Mapping, name)
	if ok {
		return unique, true
	}

	return mapping.Parent.GetUniqueName(name)
}

func GetUniqueNameMatchingVariable(mapping map[string]string, name string) (string, bool) {
	for original, unique := range mapping {
		if physical.VariableNameMatchesField(name, original) {
			return unique, true
		}
	}
	return "", false
}

func ReverseMapping(mapping map[string]string) map[string]string {
	out := make(map[string]string)
	for k, v := range mapping {
		out[v] = k
	}
	return out
}

type Node interface {
	// Typechecking panics on error, because it will never be handled in a different way than being bubbled up to the top.
	Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) (physical.Node, map[string]string)
}

type DataSource struct {
	name, alias string
	options     map[string]string
}

func NewDataSource(name, alias string, options map[string]string) *DataSource {
	return &DataSource{name: name, alias: alias, options: options}
}

func (ds *DataSource) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) (physical.Node, map[string]string) {
	if cte, ok := logicalEnv.CommonTableExpressions[ds.name]; ok {
		return cte.Node, cte.UniqueVariableMapping
	}

	if ds.name == "dual" {
		uniqueDummy := logicalEnv.GetUnique("dummy")
		return physical.Node{
				Schema: physical.NewSchema(
					[]physical.SchemaField{
						{
							Name: uniqueDummy,
							Type: octosql.String,
						},
					},
					-1,
				),
				NodeType: physical.NodeTypeInMemoryRecords,
				InMemoryRecords: &physical.InMemoryRecords{
					Records: []execution.Record{
						execution.NewRecord([]octosql.Value{octosql.NewString("X")}, false, time.Time{}),
					},
				},
			}, map[string]string{
				"dummy": uniqueDummy,
			}
	}

	datasource, schema, err := env.Datasources.GetDatasource(ctx, ds.name, ds.options)
	if err != nil {
		panic(fmt.Errorf("couldn't create datasource: %v", err))
	}

	outMapping := make(map[string]string)
	newSchemaFields := make([]physical.SchemaField, len(schema.Fields))
	copy(newSchemaFields, schema.Fields)
	for i := range newSchemaFields {
		newSchemaFields[i].Name = ds.alias + "." + newSchemaFields[i].Name
	}
	for i := range newSchemaFields {
		name := newSchemaFields[i].Name
		unique := logicalEnv.GetUnique(name)
		outMapping[name] = unique
		newSchemaFields[i].Name = unique
	}
	return physical.Node{
		Schema:   physical.NewSchema(newSchemaFields, schema.TimeField, physical.WithNoRetractions(schema.NoRetractions)),
		NodeType: physical.NodeTypeDatasource,
		Datasource: &physical.Datasource{
			Name:                     ds.name,
			Alias:                    ds.alias,
			DatasourceImplementation: datasource,
			VariableMapping:          outMapping,
		},
	}, outMapping
}

type Expression interface {
	Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Expression
}

// FieldNamer can be implemented by expressions with pretty default names based on their content.
type FieldNamer interface {
	FieldName() string
}

type StarExpression struct {
	qualifier string
}

func NewStarExpression(qualifier string) *StarExpression {
	return &StarExpression{qualifier: qualifier}
}

func (se *StarExpression) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Expression {
	panic("implement me")
}

type Variable struct {
	name string
}

func NewVariable(name string) *Variable {
	return &Variable{name: name}
}

func (v *Variable) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Expression {
	uniqueName, ok := logicalEnv.UniqueVariableNames.GetUniqueName(v.name)
	if !ok {
		panic(fmt.Errorf("unknown variable: '%s'", v.name))
	}

	isLevel0 := true
	for varCtx := env.VariableContext; varCtx != nil; varCtx = varCtx.Parent {
		for _, field := range varCtx.Fields {
			if field.Name == uniqueName {
				return physical.Expression{
					Type:           field.Type,
					ExpressionType: physical.ExpressionTypeVariable,
					Variable: &physical.Variable{
						Name:     uniqueName,
						IsLevel0: isLevel0,
					},
				}
			}
		}
		isLevel0 = false
	}
	// TODO: Expression typecheck errors should contain context. (position in input SQL)
	panic(fmt.Errorf("unknown variable: '%s'", uniqueName))
}

func (v *Variable) FieldName() string {
	return v.name
}

type Constant struct {
	value octosql.Value
}

func NewConstant(value octosql.Value) *Constant {
	return &Constant{value: value}
}

func (c *Constant) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Expression {
	return physical.Expression{
		Type:           c.value.Type(),
		ExpressionType: physical.ExpressionTypeConstant,
		Constant: &physical.Constant{
			Value: c.value,
		},
	}
}

type Tuple struct {
	expressions []Expression
}

func NewTuple(expressions []Expression) *Tuple {
	return &Tuple{expressions: expressions}
}

func (t *Tuple) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Expression {
	args := make([]physical.Expression, len(t.expressions))
	argTypes := make([]octosql.Type, len(t.expressions))
	for i := range t.expressions {
		args[i] = t.expressions[i].Typecheck(ctx, env, logicalEnv)
		argTypes[i] = args[i].Type
	}
	return physical.Expression{
		Type:           octosql.Type{TypeID: octosql.TypeIDTuple, Tuple: struct{ Elements []octosql.Type }{Elements: argTypes}},
		ExpressionType: physical.ExpressionTypeTuple,
		Tuple: &physical.Tuple{
			Arguments: args,
		},
	}
}

type And struct {
	left, right Expression
}

func NewAnd(left, right Expression) *And {
	return &And{left: left, right: right}
}

func (and *And) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Expression {
	left := TypecheckExpression(ctx, env, logicalEnv, octosql.TypeSum(octosql.Boolean, octosql.Null), and.left)
	right := TypecheckExpression(ctx, env, logicalEnv, octosql.TypeSum(octosql.Boolean, octosql.Null), and.right)
	outputType := octosql.Boolean
	if octosql.Null.Is(left.Type) == octosql.TypeRelationIs ||
		octosql.Null.Is(right.Type) == octosql.TypeRelationIs {
		outputType = octosql.TypeSum(octosql.Boolean, octosql.Null)
	}
	return physical.Expression{
		Type:           outputType,
		ExpressionType: physical.ExpressionTypeAnd,
		And: &physical.And{
			Arguments: []physical.Expression{
				left,
				right,
			},
		},
	}
}

type Or struct {
	left, right Expression
}

func NewOr(left, right Expression) *Or {
	return &Or{left: left, right: right}
}

func (or *Or) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Expression {
	left := TypecheckExpression(ctx, env, logicalEnv, octosql.TypeSum(octosql.Boolean, octosql.Null), or.left)
	right := TypecheckExpression(ctx, env, logicalEnv, octosql.TypeSum(octosql.Boolean, octosql.Null), or.right)
	outputType := octosql.Boolean
	if octosql.Null.Is(left.Type) == octosql.TypeRelationIs ||
		octosql.Null.Is(right.Type) == octosql.TypeRelationIs {
		outputType = octosql.TypeSum(octosql.Boolean, octosql.Null)
	}
	return physical.Expression{
		Type:           outputType,
		ExpressionType: physical.ExpressionTypeOr,
		Or: &physical.Or{
			Arguments: []physical.Expression{
				left,
				right,
			},
		},
	}
}

type QueryExpression struct {
	node Node
}

func NewQueryExpression(node Node) *QueryExpression {
	return &QueryExpression{node: node}
}

func (ne *QueryExpression) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Expression {
	source, mapping := ne.node.Typecheck(ctx, env, logicalEnv)
	reverseMapping := ReverseMapping(mapping)

	var elementType octosql.Type
	if len(source.Schema.Fields) == 1 {
		elementType = source.Schema.Fields[0].Type
	} else {
		structFields := make([]octosql.StructField, len(source.Schema.Fields))
		for i := range source.Schema.Fields {
			structFields[i] = octosql.StructField{
				Name: reverseMapping[source.Schema.Fields[i].Name],
				Type: source.Schema.Fields[i].Type,
			}
		}
		elementType = octosql.Type{TypeID: octosql.TypeIDStruct, Struct: struct{ Fields []octosql.StructField }{Fields: structFields}}
	}

	return physical.Expression{
		Type:           octosql.Type{TypeID: octosql.TypeIDList, List: struct{ Element *octosql.Type }{Element: &elementType}},
		ExpressionType: physical.ExpressionTypeQueryExpression,
		QueryExpression: &physical.QueryExpression{
			Source: source,
		},
	}
}

type Coalesce struct {
	args []Expression
}

func NewCoalesce(args []Expression) *Coalesce {
	return &Coalesce{args: args}
}

func (c *Coalesce) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Expression {
	if len(c.args) == 0 {
		panic("COALESCE must be provided at least 1 argument")
	}

	exprs := make([]physical.Expression, len(c.args))
	for i := range c.args {
		exprs[i] = c.args[i].Typecheck(ctx, env, logicalEnv)
	}

	outputType := exprs[0].Type
	for _, expr := range exprs[1:] {
		outputType = octosql.TypeSum(outputType, expr.Type)
	}

	return physical.Expression{
		Type:           outputType,
		ExpressionType: physical.ExpressionTypeCoalesce,
		Coalesce: &physical.Coalesce{
			Arguments: exprs,
		},
	}
}

type Cast struct {
	arg          Expression
	targetTypeID octosql.TypeID
}

func NewCast(arg Expression, targetTypeID octosql.TypeID) *Cast {
	return &Cast{arg: arg, targetTypeID: targetTypeID}
}

func (c *Cast) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Expression {
	expr := c.arg.Typecheck(ctx, env, logicalEnv)

	if expr.Type.TypeID != octosql.TypeIDUnion {
		panic(fmt.Errorf("typecast of non-union type expression '%s'", expr.Type.String()))
	}

	var targetType octosql.Type
	found := false
	for _, alternative := range expr.Type.Union.Alternatives {
		if alternative.TypeID == c.targetTypeID {
			found = true
			targetType = alternative
			break
		}
	}
	if !found {
		panic(fmt.Errorf("typecast target type '%s' isn't an alternative of the expression union type '%s'", octosql.Type{TypeID: c.targetTypeID}.String(), expr.Type.String()))
	}

	return physical.Expression{
		Type:           octosql.TypeSum(targetType, octosql.Null),
		ExpressionType: physical.ExpressionTypeCast,
		Cast: &physical.Cast{
			Expression:   expr,
			TargetTypeID: c.targetTypeID,
		},
	}
}

type ObjectFieldAccess struct {
	object Expression
	field  string
}

func NewObjectFieldAccess(object Expression, field string) *ObjectFieldAccess {
	return &ObjectFieldAccess{object: object, field: field}
}

func (c *ObjectFieldAccess) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Expression {
	expr := TypecheckPossiblyNullableStruct(ctx, env, logicalEnv, c.object)

	nonNullableExprType := octosql.NonNullable(expr.Type)
	fieldIndex := -1
	for i, field := range nonNullableExprType.Struct.Fields {
		if field.Name == c.field {
			fieldIndex = i
			break
		}
	}
	if fieldIndex == -1 {
		panic(fmt.Errorf("object field access of field '%s' on object expression of type '%s' without that field", c.field, expr.Type))
	}

	outType := nonNullableExprType.Struct.Fields[fieldIndex].Type
	if expr.Type.TypeID != octosql.TypeIDStruct {
		outType = octosql.TypeSum(outType, octosql.Null)
	}

	return physical.Expression{
		Type:           outType,
		ExpressionType: physical.ExpressionTypeObjectFieldAccess,
		ObjectFieldAccess: &physical.ObjectFieldAccess{
			Object: expr,
			Field:  c.field,
		},
	}
}

func TypecheckExpression(ctx context.Context, env physical.Environment, logicalEnv Environment, expected octosql.Type, expression Expression) physical.Expression {
	expr := expression.Typecheck(ctx, env, logicalEnv)
	rel := expr.Type.Is(expected)
	if rel == octosql.TypeRelationIsnt {
		panic(fmt.Errorf("expected %s, got %s", expected, expr.Type))
	}
	if rel == octosql.TypeRelationMaybe {
		return physical.Expression{
			Type:           *octosql.TypeIntersection(expected, expr.Type),
			ExpressionType: physical.ExpressionTypeTypeAssertion,
			TypeAssertion: &physical.TypeAssertion{
				Expression: expr,
				TargetType: expected,
			},
		}
	}

	return expr
}

func TypecheckPossiblyNullableStruct(ctx context.Context, env physical.Environment, logicalEnv Environment, expression Expression) physical.Expression {
	expr := expression.Typecheck(ctx, env, logicalEnv)
	nonNullableExprType := octosql.NonNullable(expr.Type)

	if nonNullableExprType.TypeID == octosql.TypeIDStruct {
		return expr
	}
	if nonNullableExprType.TypeID != octosql.TypeIDUnion {
		panic(fmt.Errorf("expected object or union containing object, got %s", expr.Type))
	}
	foundIndex := -1
	for i := range nonNullableExprType.Union.Alternatives {
		if nonNullableExprType.Union.Alternatives[i].TypeID == octosql.TypeIDStruct {
			foundIndex = i
			break
		}
	}
	if foundIndex == -1 {
		panic(fmt.Errorf("expected object or union containing object, got %s", expr.Type))
	}
	targetType := octosql.Type{
		TypeID: octosql.TypeIDUnion,
		Union: struct{ Alternatives []octosql.Type }{Alternatives: []octosql.Type{
			nonNullableExprType.Union.Alternatives[foundIndex],
			octosql.Null,
		}},
	}
	return physical.Expression{
		Type:           targetType,
		ExpressionType: physical.ExpressionTypeTypeAssertion,
		TypeAssertion: &physical.TypeAssertion{
			Expression: expr,
			TargetType: targetType,
		},
	}
}
