package logical

import (
	"context"
	"fmt"
	"strconv"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical"
)

type PhysicalPlanCreator struct {
	variableCounter int
	dataSourceRepo  *physical.DataSourceRepository
	physicalConfig  map[string]interface{}
}

func NewPhysicalPlanCreator(repo *physical.DataSourceRepository, physicalConfig map[string]interface{}) *PhysicalPlanCreator {
	return &PhysicalPlanCreator{
		variableCounter: 0,
		dataSourceRepo:  repo,
		physicalConfig:  physicalConfig,
	}
}

func (creator *PhysicalPlanCreator) GetVariableName() (out octosql.VariableName) {
	out = octosql.NewVariableName(fmt.Sprintf("const_%d", creator.variableCounter))
	creator.variableCounter++
	return
}

func (creator *PhysicalPlanCreator) WithCommonTableExpression(name string, nodes []physical.Node) *PhysicalPlanCreator {
	newDataSourceRepo := creator.dataSourceRepo.WithFactory(
		name,
		func(name, alias string) []physical.Node {
			out := nodes
			if len(alias) > 0 {
				for i := range out {
					out[i] = physical.NewRequalifier(alias, out[i])
				}
			}
			return out
		},
	)

	newCreator := &PhysicalPlanCreator{
		variableCounter: creator.variableCounter,
		dataSourceRepo:  newDataSourceRepo,
	}

	return newCreator
}

type OutputOptions struct {
	OrderByExpressions []Expression
	OrderByDirections  []OrderDirection
	Limit              Expression
	Offset             Expression
}

func (opts *OutputOptions) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (*physical.OutputOptions, octosql.Variables, error) {
	orderByExpressions := make([]physical.Expression, len(opts.OrderByExpressions))
	variables := octosql.NoVariables()
	for i := range opts.OrderByExpressions {
		physicalExpr, exprVariables, err := opts.OrderByExpressions[i].Physical(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrapf(
				err,
				"couldn't get physical plan for order by expression with index %d", i,
			)
		}
		variables, err = variables.MergeWith(exprVariables)
		if err != nil {
			return nil, nil, errors.Wrapf(
				err,
				"couldn't merge variables with those of order by expression with index %d", i,
			)
		}

		orderByExpressions[i] = physicalExpr
	}

	orderByDirections := make([]physical.OrderDirection, len(opts.OrderByDirections))
	for i, dir := range opts.OrderByDirections {
		orderByDirections[i] = physical.OrderDirection(dir)
	}

	var limit physical.Expression
	if opts.Limit != nil {
		limitExpression, limitVariables, err := opts.Limit.Physical(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't get physical plan for limit expression")
		}
		variables, err = variables.MergeWith(limitVariables)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't merge variables with those of limit expression")
		}
		limit = limitExpression
	}

	var offset physical.Expression
	if opts.Offset != nil {
		offsetExpression, offsetVariables, err := opts.Offset.Physical(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrap(err, "couldn't get physical plan for offset expression")
		}
		variables, err = variables.MergeWith(offsetVariables)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't merge variables with those of offset expression")
		}
		offset = offsetExpression
	}

	return physical.NewOutputOptions(orderByExpressions, orderByDirections, limit, offset), variables, nil
}

type Node interface {
	graph.Visualizer

	Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) ([]physical.Node, octosql.Variables, error)
}

type DataSource struct {
	name  string
	alias string
}

func NewDataSource(name string, alias string) *DataSource {
	return &DataSource{name: name, alias: alias}
}

func (ds *DataSource) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) ([]physical.Node, octosql.Variables, error) {
	outDs, err := physicalCreator.dataSourceRepo.Get(ds.name, ds.alias)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get data source")
	}
	return outDs, octosql.NoVariables(), nil
}

func (ds *DataSource) Visualize() *graph.Node {
	n := graph.NewNode("DataSource")
	n.AddField("name", ds.name)
	n.AddField("alias", ds.alias)
	return n
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

type StarExpression struct {
	qualifier string
}

func NewStarExpression(qualifier string) *StarExpression {
	return &StarExpression{qualifier: qualifier}
}

func (se *StarExpression) Name() octosql.VariableName {
	if se.qualifier == "" {
		return octosql.StarExpressionName
	}

	return octosql.NewVariableName(fmt.Sprintf("%s.%s", se.qualifier, octosql.StarExpressionName))
}

func (se *StarExpression) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, octosql.Variables, error) {
	return se.PhysicalNamed(ctx, physicalCreator)
}

func (se *StarExpression) PhysicalNamed(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.NamedExpression, octosql.Variables, error) {
	return physical.NewStarExpression(se.qualifier), octosql.NoVariables(), nil
}

func (se *StarExpression) Visualize() *graph.Node {
	n := graph.NewNode("Star Expression")
	n.AddField("qualifier", se.qualifier)
	return n
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

func (v *Variable) Visualize() *graph.Node {
	n := graph.NewNode("Variable")
	n.AddField("name", v.name.String())
	return n
}

type Constant struct {
	value interface{}
}

func NewConstant(value interface{}) *Constant {
	return &Constant{value: value}
}

func (c *Constant) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, octosql.Variables, error) {
	name := physicalCreator.GetVariableName()
	return physical.NewVariable(name), octosql.NewVariables(map[octosql.VariableName]octosql.Value{
		name: octosql.NormalizeType(c.value),
	}), nil
}

func (c *Constant) Visualize() *graph.Node {
	n := graph.NewNode("Constant")
	n.AddField("value", fmt.Sprintf("%T %v", c.value, c.value))
	return n
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

func (tup *Tuple) Visualize() *graph.Node {
	n := graph.NewNode("Tuple")
	if len(tup.expressions) != 0 {
		for idx, expr := range tup.expressions {
			n.AddChild("expr_"+strconv.Itoa(idx), expr.Visualize())
		}
	}
	return n
}

type NodeExpression struct {
	node Node
}

func NewNodeExpression(node Node) *NodeExpression {
	return &NodeExpression{node: node}
}

func (ne *NodeExpression) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, octosql.Variables, error) {
	sourceNodes, variables, err := ne.node.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for node expression")
	}

	outNodes := physical.NewShuffle(1, physical.NewConstantStrategy(0), sourceNodes)

	return physical.NewNodeExpression(outNodes[0]), variables, nil
}

func (ne *NodeExpression) Visualize() *graph.Node {
	n := graph.NewNode("Node Expression")
	if ne.node != nil {
		n.AddChild("source", ne.node.Visualize())
	}
	return n
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

func (le *LogicExpression) Visualize() *graph.Node {
	n := graph.NewNode("Logic Expression")
	if le.formula != nil {
		n.AddChild("source", le.formula.Visualize())
	}
	return n
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

func (alExpr *AliasedExpression) Visualize() *graph.Node {
	n := graph.NewNode("Aliased Expression")
	n.AddField("alias", string(alExpr.name))
	if alExpr.expr != nil {
		n.AddChild("expr", alExpr.expr.Visualize())
	}
	return n
}
