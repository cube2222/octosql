package physical

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
)

type Node interface {
	Materialize(ctx context.Context) execution.Node
}

type Expression interface {
	Materialize(ctx context.Context) execution.Expression
}

type Variable struct {
	name octosql.VariableName
}

func NewVariable(name octosql.VariableName) *Variable {
	return &Variable{name: name}
}

func (v *Variable) Materialize(ctx context.Context) execution.Expression {
	return execution.NewVariable(v.name)
}

type NodeExpression struct {
	name octosql.VariableName
	node Node
}

func NewNodeExpression(name octosql.VariableName, node Node) *NodeExpression {
	return &NodeExpression{name: name, node: node}
}

func (ne *NodeExpression) Materialize(ctx context.Context) execution.Expression {
	return execution.NewNodeExpression(ne.name, ne.node.Materialize(ctx))
}
