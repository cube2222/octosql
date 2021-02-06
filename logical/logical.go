package logical

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
)

type CommonTableExpressionsRepository struct {
}

func (ctes *CommonTableExpressionsRepository) WithCommonTableExpression(name string, nodes []physical.Node) *CommonTableExpressionsRepository {
	panic("implement me")
	// newDataSourceRepo := ctes.dataSourceRepo.WithFactory(
	// 	name,
	// 	func(name, alias string) []physical.Node {
	// 		out := nodes
	// 		if len(alias) > 0 {
	// 			for i := range out {
	// 				out[i] = physical.NewRequalifier(alias, out[i])
	// 			}
	// 		}
	// 		return out
	// 	},
	// )
	//
	// newCreator := &PhysicalPlanCreator{
	// 	variableCounter: creator.variableCounter,
	// 	dataSourceRepo:  newDataSourceRepo,
	// }
	//
	// return newCreator
}

type Node interface {
	Typecheck(ctx context.Context, env physical.Environment, state physical.State) ([]physical.Node, error)
}

type DataSource struct {
	name  string
	alias string
}

func NewDataSource(name string, alias string) *DataSource {
	return &DataSource{name: name, alias: alias}
}

func (ds *DataSource) Typecheck(ctx context.Context, env physical.Environment, state physical.State) ([]physical.Node, error) {
	panic("implement me")
}

type Expression interface {
	Typecheck(ctx context.Context, env physical.Environment, state physical.State) (physical.Expression, error)
}

type StarExpression struct {
	qualifier string
}

func NewStarExpression(qualifier string) *StarExpression {
	return &StarExpression{qualifier: qualifier}
}

func (se *StarExpression) Typecheck(ctx context.Context, env physical.Environment, state physical.State) (physical.Expression, error) {
	panic("implement me")
}

type Variable struct {
	name string
}

func NewVariable(name string) *Variable {
	return &Variable{name: name}
}

func (v *Variable) Typecheck(ctx context.Context, env physical.Environment, state physical.State) (physical.Expression, error) {
	for varCtx := env.VariableContext; varCtx != nil; varCtx = varCtx.Parent {
		for _, field := range varCtx.Fields {
			if field.Name == v.name {
				return physical.Expression{
					Type:           field.Type,
					ExpressionType: physical.ExpressionTypeVariable,
					Variable: &physical.Variable{
						Name: v.name,
					},
				}, nil
			}
		}
	}
	return physical.Expression{}, fmt.Errorf("unkown variable: '%s'", v.name)
}

type Constant struct {
	value interface{}
}

func NewConstant(value interface{}) *Constant {
	return &Constant{value: value}
}

func (c *Constant) Typecheck(ctx context.Context, env physical.Environment, state physical.State) (physical.Expression, error) {
	panic("implement me")
}

type Tuple struct {
	expressions []Expression
}

func NewTuple(expressions []Expression) *Tuple {
	return &Tuple{expressions: expressions}
}

func (tup *Tuple) Typecheck(ctx context.Context, env physical.Environment, state physical.State) (physical.Expression, error) {
	panic("implement me")
}

type NodeExpression struct {
	node Node
}

func NewNodeExpression(node Node) *NodeExpression {
	return &NodeExpression{node: node}
}

func (ne *NodeExpression) Typecheck(ctx context.Context, env physical.Environment, state physical.State) (physical.Expression, error) {
	panic("implement me")
}

type And struct {
	left, right Expression
}

func NewAnd(left, right Expression) *And {
	return &And{left: left, right: right}
}

func (and *And) Typecheck(ctx context.Context, env physical.Environment, state physical.State) (physical.Expression, error) {
	left, err := TypecheckExpression(ctx, env, state, octosql.Boolean, and.left)
	if err != nil {
		return physical.Expression{}, err
	}
	right, err := TypecheckExpression(ctx, env, state, octosql.Boolean, and.right)
	if err != nil {
		return physical.Expression{}, err
	}
	return physical.Expression{
		Type:           octosql.Boolean,
		ExpressionType: physical.ExpressionTypeAnd,
		And: &physical.And{
			Arguments: []physical.Expression{left, right},
		},
	}, nil
}

type Or struct {
	left, right Expression
}

func NewOr(left, right Expression) *Or {
	return &Or{left: left, right: right}
}

func (or *Or) Typecheck(ctx context.Context, env physical.Environment, state physical.State) (physical.Expression, error) {
	left, err := TypecheckExpression(ctx, env, state, octosql.Boolean, or.left)
	if err != nil {
		return physical.Expression{}, err
	}
	right, err := TypecheckExpression(ctx, env, state, octosql.Boolean, or.right)
	if err != nil {
		return physical.Expression{}, err
	}
	return physical.Expression{
		Type:           octosql.Boolean,
		ExpressionType: physical.ExpressionTypeOr,
		Or: &physical.Or{
			Arguments: []physical.Expression{left, right},
		},
	}, nil
}

func TypecheckExpression(ctx context.Context, env physical.Environment, state physical.State, expected octosql.Type, expression Expression) (physical.Expression, error) {
	expr, err := expression.Typecheck(ctx, env, state)
	if err != nil {
		return physical.Expression{}, err
	}
	rel := expr.Type.Is(expected)
	if rel == octosql.TypeRelationIsnt {
		return physical.Expression{}, fmt.Errorf("expected %s, got %s", expected, expr.Type)
	}
	if rel == octosql.TypeRelationMaybe {
		return physical.Expression{
			Type:           expected,
			ExpressionType: physical.ExpressionTypeTypeAssertion,
			TypeAssertion: &physical.TypeAssertion{
				Expression: expr,
				TargetType: expected,
			},
		}, nil
	}

	return expr, nil
}
