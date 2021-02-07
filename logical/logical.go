package logical

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type Environment struct {
	CommonTableExpressions map[string]physical.Node
}

type Node interface {
	// Typechecking panics on error, because it will never be handled otherwise than being bubbled up to the top.
	Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Node
}

type DataSource struct {
	name string
}

func NewDataSource(name string) *DataSource {
	return &DataSource{name: name}
}

func (ds *DataSource) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Node {
	if node, ok := logicalEnv.CommonTableExpressions[ds.name]; ok {
		return node
	}

	datasource, err := env.Datasources.GetDatasource(ds.name)
	if err != nil {
		panic(fmt.Errorf("couldn't create datasource: %v", err))
	}
	schema, err := datasource.Schema()
	if err != nil {
		panic(fmt.Errorf("couldn't get datasource schema: %v", err))
	}
	return physical.Node{
		Schema:   schema,
		NodeType: physical.NodeTypeDatasource,
		Datasource: &physical.Datasource{
			Name:                     ds.name,
			DatasourceImplementation: datasource,
		},
	}
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
	isLevel0 := true
	for varCtx := env.VariableContext; varCtx != nil; varCtx = varCtx.Parent {
		for _, field := range varCtx.Fields {
			if physical.VariableNameMatchesField(v.name, field.Name) {
				return physical.Expression{
					Type:           field.Type,
					ExpressionType: physical.ExpressionTypeVariable,
					Variable: &physical.Variable{
						Name:     v.name,
						IsLevel0: isLevel0,
					},
				}
			}
		}
		isLevel0 = false
	}
	// TODO: Expression typecheck errors should contain context. (position in input SQL)
	panic(fmt.Errorf("unkown variable: '%s'", v.name))
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
		Type:           c.value.Type,
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

func (tup *Tuple) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Expression {
	panic("implement me")
}

type NodeExpression struct {
	node Node
}

func NewNodeExpression(node Node) *NodeExpression {
	return &NodeExpression{node: node}
}

func (ne *NodeExpression) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Expression {
	panic("implement me")
}

type And struct {
	left, right Expression
}

func NewAnd(left, right Expression) *And {
	return &And{left: left, right: right}
}

func (and *And) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) physical.Expression {
	return physical.Expression{
		Type:           octosql.Boolean,
		ExpressionType: physical.ExpressionTypeAnd,
		And: &physical.And{
			Arguments: []physical.Expression{
				TypecheckExpression(ctx, env, logicalEnv, octosql.Boolean, and.left),
				TypecheckExpression(ctx, env, logicalEnv, octosql.Boolean, and.right),
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
	return physical.Expression{
		Type:           octosql.Boolean,
		ExpressionType: physical.ExpressionTypeOr,
		Or: &physical.Or{
			Arguments: []physical.Expression{
				TypecheckExpression(ctx, env, logicalEnv, octosql.Boolean, or.left),
				TypecheckExpression(ctx, env, logicalEnv, octosql.Boolean, or.right),
			},
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
			Type:           expected,
			ExpressionType: physical.ExpressionTypeTypeAssertion,
			TypeAssertion: &physical.TypeAssertion{
				Expression: expr,
				TargetType: expected,
			},
		}
	}

	return expr
}
