package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

type Node interface {
	Get(variables octosql.Variables) (RecordStream, error)
}

type Expression interface {
	ExpressionValue(variables octosql.Variables) (octosql.Value, error)
}

type NamedExpression interface {
	Expression
	Name() octosql.VariableName
}

type Variable struct {
	name octosql.VariableName
}

func NewVariable(name octosql.VariableName) *Variable {
	return &Variable{name: name}
}

func (v *Variable) ExpressionValue(variables octosql.Variables) (octosql.Value, error) {
	val, err := variables.Get(v.name)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't get variable %+v, available variables %+v", v.name, variables)
	}
	return val, nil
}

func (v *Variable) Name() octosql.VariableName {
	return v.name
}

type TupleExpression struct {
	expressions []Expression
}

func NewTuple(expressions []Expression) *TupleExpression {
	return &TupleExpression{expressions: expressions}
}

func (tup *TupleExpression) ExpressionValue(variables octosql.Variables) (octosql.Value, error) {
	outValues := make(octosql.Tuple, len(tup.expressions))
	for i, expr := range tup.expressions {
		value, err := expr.ExpressionValue(variables)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't get tuple subexpression with index %v", i)
		}
		outValues[i] = value
	}

	return outValues, nil
}

type NodeExpression struct {
	node Node
}

func NewNodeExpression(node Node) *NodeExpression {
	return &NodeExpression{node: node}
}

func (ne *NodeExpression) ExpressionValue(variables octosql.Variables) (octosql.Value, error) {
	records, err := ne.node.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get record stream")
	}

	var firstRecord octosql.Tuple
	outRecords := make(octosql.Tuple, 0)

	var curRecord *Record
	for curRecord, err = records.Next(); err == nil; curRecord, err = records.Next() {
		if firstRecord == nil {
			firstRecord = curRecord.AsTuple()
		}
		outRecords = append(outRecords, curRecord.AsTuple())
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
	if len(firstRecord.AsSlice()) > 1 {
		return firstRecord, nil
	}
	if len(firstRecord.AsSlice()) == 0 {
		return nil, nil
	}

	// There is exactly one field
	return firstRecord.AsSlice()[0], nil
}

type LogicExpression struct {
	formula Formula
}

func NewLogicExpression(formula Formula) *LogicExpression {
	return &LogicExpression{
		formula: formula,
	}
}

func (le *LogicExpression) ExpressionValue(variables octosql.Variables) (octosql.Value, error) {
	out, err := le.formula.Evaluate(variables)
	return octosql.MakeBool(out), err
}

type AliasedExpression struct {
	name octosql.VariableName
	expr Expression
}

func NewAliasedExpression(name octosql.VariableName, expr Expression) *AliasedExpression {
	return &AliasedExpression{name: name, expr: expr}
}

func (alExpr *AliasedExpression) ExpressionValue(variables octosql.Variables) (octosql.Value, error) {
	return alExpr.expr.ExpressionValue(variables)
}

func (alExpr *AliasedExpression) Name() octosql.VariableName {
	return alExpr.name
}
