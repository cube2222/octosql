package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type Node interface {
	Get(variables octosql.Variables) (RecordStream, error)
}

type Expression interface {
	ExpressionValue(variables octosql.Variables) (interface{}, error)
	Name() octosql.VariableName
}

type Variable struct {
	name octosql.VariableName
}

func NewVariable(name octosql.VariableName) *Variable {
	return &Variable{name: name}
}

func (v *Variable) ExpressionValue(variables octosql.Variables) (interface{}, error) {
	val, err := variables.Get(v.name)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't get variable %+v, available variables %+v", v.name, variables)
	}
	return val, nil
}

func (v *Variable) Name() octosql.VariableName {
	return v.name
}

type NodeExpression struct {
	name octosql.VariableName
	node Node
}

func NewNodeExpression(name octosql.VariableName, node Node) *NodeExpression {
	return &NodeExpression{name: name, node: node}
}

func (ne *NodeExpression) ExpressionValue(variables octosql.Variables) (interface{}, error) {
	records, err := ne.node.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get record stream")
	}

	outRecords := make([]Record, 0)

	for record, err := records.Next(); err == nil; record, err = records.Next() {
		outRecords = append(outRecords, *record)
	}
	if err != ErrEndOfStream {
		return nil, errors.Wrap(err, "couldn't get records from stream")
	}

	if len(outRecords) > 1 {
		return outRecords, nil
	}
	if len(outRecords) == 0 {
		return nil, nil
	}

	// There is exactly one record
	record := outRecords[0]
	if len(record.Fields()) > 1 {
		return record, nil
	}
	if len(record.Fields()) == 0 {
		return nil, nil
	}

	// There is exactly one field
	return record.Value(record.Fields()[0].Name), nil
}

func (ne *NodeExpression) Name() octosql.VariableName {
	return ne.name
}
