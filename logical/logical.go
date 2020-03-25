package logical

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
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

type Node interface {
	graph.Visualizer

	Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, octosql.Variables, error)
}

type DataSource struct {
	name  string
	alias string
}

func (dataSource *DataSource) Visualize() *graph.Node {
	n := graph.NewNode("DataSource")
	n.AddField("name", dataSource.name)
	n.AddField("alias", dataSource.alias)
	return n
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
	graph.Visualizer

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

func (variable *Variable) Visualize() *graph.Node {
	n := graph.NewNode("Variable")
	n.AddField("name", string(variable.name))
	return n
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

func (constant *Constant) Visualize() *graph.Node {
	n := graph.NewNode("Constant")
	n.AddField("value", fmt.Sprintf("%T %v", constant.value, constant.value))
	return n
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

func (tuple *Tuple) Visualize() *graph.Node {
	n := graph.NewNode("Tuple")
	if len(tuple.expressions) != 0 {
		for idx, expr := range tuple.expressions {
			n.AddChild("expr_"+strconv.Itoa(idx), expr.Visualize())
		}
	}
	return n
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

func (nodeExpression *NodeExpression) Visualize() *graph.Node {
	n := graph.NewNode("Node Expression")
	if nodeExpression.node != nil {
		n.AddChild("source", nodeExpression.node.Visualize())
	}
	return n
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

func (logicExpression *LogicExpression) Visualize() *graph.Node {
	n := graph.NewNode("Logic Expression")
	if logicExpression.formula != nil {
		n.AddChild("source", logicExpression.formula.Visualize())
	}
	return n
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

func (aliasedExpression *AliasedExpression) Visualize() *graph.Node {
	n := graph.NewNode("Aliased Expression")
	n.AddField("alias", string(aliasedExpression.name))
	if aliasedExpression.expr != nil {
		n.AddChild("expr", aliasedExpression.expr.Visualize())
	}
	return n
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
