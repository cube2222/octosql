package execution

import (
	"context"
	"fmt"
	"time"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"

	"github.com/pkg/errors"
)

// This struct represents additional metadata to be returned with Get() and used recursively (like WatermarkSource)
type ExecutionOutput struct {
	WatermarkSource WatermarkSource
}

func NewExecutionOutput(ws WatermarkSource) *ExecutionOutput {
	return &ExecutionOutput{
		WatermarkSource: ws,
	}
}

type ZeroWatermarkGenerator struct {
}

func (s *ZeroWatermarkGenerator) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	return time.Time{}, nil
}

func NewZeroWatermarkGenerator() *ZeroWatermarkGenerator {
	return &ZeroWatermarkGenerator{}
}

type Node interface {
	Get(ctx context.Context, variables octosql.Variables, streamID *StreamID) (RecordStream, *ExecutionOutput, error)
}

type Expression interface {
	ExpressionValue(ctx context.Context, variables octosql.Variables) (octosql.Value, error)
}

type NamedExpression interface {
	Expression
	Name() octosql.VariableName
}

type StarExpression struct {
	qualifier string
}

func NewStarExpression(qualifier string) *StarExpression {
	return &StarExpression{qualifier: qualifier}
}

// In FieldsAndValues, ExpressionValue and Name we ignore errors from Get
// because we know that the fields we get are the ones that are surely
// in the variables. Maybe we can panic here.
func (se *StarExpression) FieldsAndValues(variables octosql.Variables) ([]octosql.VariableName, []octosql.Value) {
	values := make([]octosql.Value, 0)
	fields := se.Fields(variables)

	for _, field := range fields {
		value, _ := variables.Get(field)
		values = append(values, value)
	}

	return fields, values
}

func (se *StarExpression) Fields(variables octosql.Variables) []octosql.VariableName {
	keys := variables.DeterministicOrder()
	fields := make([]octosql.VariableName, 0)

	for _, key := range keys {
		if se.doesVariableMatch(key) {
			fields = append(fields, key)
		}
	}

	return fields
}

func (se *StarExpression) ExpressionValue(ctx context.Context, variables octosql.Variables) (octosql.Value, error) {
	_, values := se.FieldsAndValues(variables)
	return octosql.MakeTuple(values), nil
}

func (se *StarExpression) Name() octosql.VariableName {
	if se.qualifier == "" {
		return octosql.StarExpressionName
	}

	return octosql.NewVariableName(fmt.Sprintf("%s.%s", se.qualifier, octosql.StarExpressionName))
}

func (se *StarExpression) doesVariableMatch(vname octosql.VariableName) bool {
	return se.qualifier == "" || se.qualifier == vname.Source()
}

type Variable struct {
	name octosql.VariableName
}

func NewVariable(name octosql.VariableName) *Variable {
	return &Variable{name: name}
}

func (v *Variable) ExpressionValue(ctx context.Context, variables octosql.Variables) (octosql.Value, error) {
	val, err := variables.Get(v.name)
	if err != nil {
		return octosql.ZeroValue(), errors.Wrapf(err, "couldn't get variable %+v, available variables %+v", v.name, variables)
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

func (tup *TupleExpression) ExpressionValue(ctx context.Context, variables octosql.Variables) (octosql.Value, error) {
	outValues := make([]octosql.Value, len(tup.expressions))
	for i, expr := range tup.expressions {
		value, err := expr.ExpressionValue(ctx, variables)
		if err != nil {
			return octosql.ZeroValue(), errors.Wrapf(err, "couldn't get tuple subexpression with index %v", i)
		}
		outValues[i] = value
	}

	return octosql.MakeTuple(outValues), nil
}

type NodeExpression struct {
	node         Node
	stateStorage storage.Storage
}

func NewNodeExpression(node Node, stateStorage storage.Storage) *NodeExpression {
	return &NodeExpression{node: node, stateStorage: stateStorage}
}

func (ne *NodeExpression) ExpressionValue(ctx context.Context, variables octosql.Variables) (octosql.Value, error) {
	tx := ne.stateStorage.BeginTransaction()
	// TODO: All of this has to be rewritten to be multithreaded using background jobs for the subqueries. Think about this.
	recordStream, _, err := ne.node.Get(storage.InjectStateTransaction(ctx, tx), variables, GetRawStreamID())
	if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get record stream")
	}
	if err := tx.Commit(); err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't commit transaction")
	}

	records, err := ReadAll(ctx, ne.stateStorage, recordStream)
	if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't read whole subquery stream")
	}

	var firstRecord octosql.Value
	outRecords := make([]octosql.Value, 0)
	for _, curRecord := range records {
		if octosql.AreEqual(firstRecord, octosql.ZeroValue()) {
			firstRecord = curRecord.AsTuple()
		}
		outRecords = append(outRecords, curRecord.AsTuple())
	}

	if len(outRecords) > 1 {
		return octosql.MakeTuple(outRecords), nil
	}
	if len(outRecords) == 0 {
		return octosql.ZeroValue(), nil
	}

	// There is exactly one record
	if len(firstRecord.AsSlice()) > 1 {
		return firstRecord, nil
	}
	if len(firstRecord.AsSlice()) == 0 {
		return octosql.ZeroValue(), nil
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

func (le *LogicExpression) ExpressionValue(ctx context.Context, variables octosql.Variables) (octosql.Value, error) {
	out, err := le.formula.Evaluate(ctx, variables)
	return octosql.MakeBool(out), err
}

type AliasedExpression struct {
	name octosql.VariableName
	expr Expression
}

func NewAliasedExpression(name octosql.VariableName, expr Expression) *AliasedExpression {
	return &AliasedExpression{name: name, expr: expr}
}

func (alExpr *AliasedExpression) ExpressionValue(ctx context.Context, variables octosql.Variables) (octosql.Value, error) {
	return alExpr.expr.ExpressionValue(ctx, variables)
}

func (alExpr *AliasedExpression) Name() octosql.VariableName {
	return alExpr.name
}
