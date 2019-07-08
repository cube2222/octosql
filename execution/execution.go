package execution

import (
	"github.com/cube2222/octosql"
	"github.com/pkg/errors"
)

// TODO: Temporary. Delete me.
func (*Record) OctoValue() {}

type RecordSliceValue []Record

func (RecordSliceValue) OctoValue() {}

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
		value, err := extractSingleValue(expr, variables)
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

	outRecords := make(RecordSliceValue, 0)

	var curRecord *Record
	for curRecord, err = records.Next(); err == nil; curRecord, err = records.Next() {
		outRecords = append(outRecords, *curRecord)
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
	record := &outRecords[0]
	if len(record.Fields()) > 1 {
		return record, nil
	}
	if len(record.Fields()) == 0 {
		return nil, nil
	}

	// There is exactly one field
	return record.Value(record.Fields()[0].Name), nil
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

func extractSingleValue(expr Expression, variables octosql.Variables) (octosql.Value, error) {
	value, err := expr.ExpressionValue(variables)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't get expression's value")
	}

	if records, ok := value.(RecordSliceValue); ok {
		if len(records) != 1 {
			return nil, errors.Errorf("number of records different than 1: %+v", value)
		}
		value = &records[0]
	}

	if record, ok := value.(*Record); ok {
		if len(record.Fields()) > 1 {
			return nil, errors.Errorf("multi field record ended up in single expression field %+v", value)
		}
		return record.Value(record.Fields()[0].Name), nil
	}
	return value, nil
}
