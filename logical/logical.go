package logical

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql"
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
	out = octosql.VariableName(fmt.Sprintf("const_%d", creator.variableCounter))
	creator.variableCounter++
	return
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

type Variable struct {
	name octosql.VariableName
}

func NewVariable(name octosql.VariableName) *Variable {
	return &Variable{name: name}
}

func (v *Variable) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, octosql.Variables, error) {
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
	return physical.NewVariable(name), octosql.NewVariables(map[octosql.VariableName]interface{}{
		name: v.value,
	}), nil
}

type NodeExpression struct {
	name octosql.VariableName
	node Node
}

func NewNodeExpression(name octosql.VariableName, node Node) *NodeExpression {
	return &NodeExpression{name: name, node: node}
}

func (ne *NodeExpression) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, octosql.Variables, error) {
	physicalNode, variables, err := ne.node.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for node expression")
	}
	return physical.NewNodeExpression(ne.name, physicalNode), variables, nil
}
