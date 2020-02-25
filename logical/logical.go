package logical

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
)

type PhysicalPlanCreator struct {
	variableCounter int
	dataSourceRepo  *physical.DataSourceRepository
}

func NewPhysicalPlanCreator(repo *physical.DataSourceRepository) *PhysicalPlanCreator {
	return &PhysicalPlanCreator{
		variableCounter: 0,
		dataSourceRepo:  repo,
	}
}

func (creator *PhysicalPlanCreator) GetVariableName() (out octosql.VariableName) {
	out = octosql.NewVariableName(fmt.Sprintf("const_%d", creator.variableCounter))
	creator.variableCounter++
	return
}

func (creator *PhysicalPlanCreator) WithCommonTableExpression(name string, node physical.Node) *PhysicalPlanCreator {
	newDataSourceRepo := creator.dataSourceRepo.WithFactory(
		name,
		func(name, alias string) physical.Node {
			return physical.NewRequalifier(alias, node)
		},
	)

	newCreator := &PhysicalPlanCreator{
		variableCounter: creator.variableCounter,
		dataSourceRepo:  newDataSourceRepo,
	}

	return newCreator
}

type Node interface {
	Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, octosql.Variables, error)
}

type DataSource struct {
	name  string
	alias string
}

func NewDataSource(name string, alias string) *DataSource {
	return &DataSource{name: name, alias: alias}
}

func (ds *DataSource) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, octosql.Variables, error) {
	outDs, err := physicalCreator.dataSourceRepo.Get(ds.name, ds.alias)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get data source")
	}
	return outDs, octosql.NoVariables(), nil
}

type Expression interface {
	Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, octosql.Variables, error)
}

type NamedExpression interface {
	Expression
	Name() octosql.VariableName
	PhysicalNamed(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.NamedExpression, octosql.Variables, error)
}

type Variable struct {
	name octosql.VariableName
}

func NewVariable(name octosql.VariableName) *Variable {
	return &Variable{name: name}
}

func (v *Variable) Name() octosql.VariableName {
	return v.name
}

func (v *Variable) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, octosql.Variables, error) {
	return v.PhysicalNamed(ctx, physicalCreator)
}

func (v *Variable) PhysicalNamed(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.NamedExpression, octosql.Variables, error) {
	return physical.NewVariable(v.name), octosql.NoVariables(), nil
}

type Constant struct {
	value interface{}
}

func NewConstant(value interface{}) *Constant {
	return &Constant{value: value}
}

func (v *Constant) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, octosql.Variables, error) {
	name := physicalCreator.GetVariableName()
	return physical.NewVariable(name), octosql.NewVariables(map[octosql.VariableName]octosql.Value{
		name: octosql.NormalizeType(v.value),
	}), nil
}

type Tuple struct {
	expressions []Expression
}

func NewTuple(expressions []Expression) *Tuple {
	return &Tuple{expressions: expressions}
}

func (tup *Tuple) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, octosql.Variables, error) {
	physicalExprs := make([]physical.Expression, len(tup.expressions))
	variables := octosql.NoVariables()
	for i := range tup.expressions {
		physicalExpr, exprVariables, err := tup.expressions[i].Physical(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrapf(
				err,
				"couldn't get physical plan for tuple subexpression with index %d", i,
			)
		}
		variables, err = variables.MergeWith(exprVariables)
		if err != nil {
			return nil, nil, errors.Wrapf(
				err,
				"couldn't merge variables with those of tuple subexpression with index %d", i,
			)
		}

		physicalExprs[i] = physicalExpr
	}
	return physical.NewTuple(physicalExprs), variables, nil
}

type NodeExpression struct {
	node Node
}

func NewNodeExpression(node Node) *NodeExpression {
	return &NodeExpression{node: node}
}

func (ne *NodeExpression) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, octosql.Variables, error) {
	physicalNode, variables, err := ne.node.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for node expression")
	}
	return physical.NewNodeExpression(physicalNode), variables, nil
}

type LogicExpression struct {
	formula Formula
}

func NewLogicExpression(formula Formula) *LogicExpression {
	return &LogicExpression{formula: formula}
}

func (le *LogicExpression) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, octosql.Variables, error) {
	physicalNode, variables, err := le.formula.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for logic expression")
	}
	return physical.NewLogicExpression(physicalNode), variables, nil
}

type AliasedExpression struct {
	name octosql.VariableName
	expr Expression
}

func NewAliasedExpression(name octosql.VariableName, expr Expression) NamedExpression {
	return &AliasedExpression{name: name, expr: expr}
}

func (alExpr *AliasedExpression) Name() octosql.VariableName {
	return alExpr.name
}

func (alExpr *AliasedExpression) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, octosql.Variables, error) {
	return alExpr.PhysicalNamed(ctx, physicalCreator)
}

func (alExpr *AliasedExpression) PhysicalNamed(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.NamedExpression, octosql.Variables, error) {
	physicalNode, variables, err := alExpr.expr.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for aliased expression")
	}
	return physical.NewAliasedExpression(alExpr.name, physicalNode), variables, nil
}
