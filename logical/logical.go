package logical

import (
	"context"

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
	panic("implement me")
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
