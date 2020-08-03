package execution

import (
	"context"
	"fmt"
	"time"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"

	"github.com/pkg/errors"
)

type Task func() error

// This struct represents additional metadata to be returned with Get() and used recursively (like WatermarkSource)
type ExecutionOutput struct {
	// Watermark source is the highest (in the execution tree)
	// watermark source available, which the record consumer should consume.
	WatermarkSource WatermarkSource

	// Next shuffles contains information about the next shuffles down the execution plan
	// which need to be started.
	NextShuffles map[string]ShuffleData

	// Tasks to run are functions which need to be run asynchronously,
	// after the storage initialization has been committed (and will thus be available for reading).
	TasksToRun []Task
}

type ShuffleData struct {
	ShuffleID *ShuffleID
	Shuffle   *Shuffle
	Variables octosql.Variables
}

func mergeNextShuffles(first, second map[string]ShuffleData) (map[string]ShuffleData, error) {
	result := first

	for key, value := range second {
		result[key] = value
	}

	return result, nil
}

func NewExecutionOutput(ws WatermarkSource, nextShuffles map[string]ShuffleData, tasksToRun []Task) *ExecutionOutput {
	return &ExecutionOutput{
		WatermarkSource: ws,
		NextShuffles:    nextShuffles,
		TasksToRun:      tasksToRun,
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

type UnionWatermarkGenerator struct {
	sources []WatermarkSource
}

func NewUnionWatermarkGenerator(sources []WatermarkSource) *UnionWatermarkGenerator {
	return &UnionWatermarkGenerator{
		sources: sources,
	}
}

func (uwg *UnionWatermarkGenerator) GetWatermark(ctx context.Context, tx storage.StateTransaction) (time.Time, error) {
	minimalTime := MaxWatermark

	for i := range uwg.sources {
		sourceTime, err := uwg.sources[i].GetWatermark(ctx, tx)
		if err != nil {
			return minimalTime, errors.Wrapf(err, "couldn't get watermark for source with id %v", i)
		}

		if minimalTime.After(sourceTime) {
			minimalTime = sourceTime
		}
	}

	return minimalTime, nil
}

type OutputOptions struct {
	OrderByExpressions []Expression
	OrderByDirections  []OrderDirection
	Limit              Expression
	Offset             Expression
}

func NewOutputOptions(
	orderByExpressions []Expression,
	orderByDirections []OrderDirection,
	limit Expression,
	offset Expression,
) *OutputOptions {
	return &OutputOptions{
		OrderByExpressions: orderByExpressions,
		OrderByDirections:  orderByDirections,
		Limit:              limit,
		Offset:             offset,
	}
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
	fields := se.Fields(variables)
	values := make([]octosql.Value, len(fields))

	for i := range fields {
		value, err := variables.Get(fields[i])
		if err != nil {
			panic("failed to read star expression")
		}
		values[i] = value
	}

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

type RecordExpression struct{}

func NewRecordExpression() Expression {
	return &RecordExpression{}
}

func (re *RecordExpression) ExpressionValue(ctx context.Context, variables octosql.Variables) (octosql.Value, error) {
	fields := variables.DeterministicOrder()
	fieldValues := make([]octosql.Value, 0)
	values := make([]octosql.Value, 0)

	for _, f := range fields {
		if f.Source() == SystemSource { // TODO: some better way to do this?
			continue
		}

		v, _ := variables.Get(f)
		values = append(values, v)
		fieldValues = append(fieldValues, octosql.MakeString(f.String()))
	}

	fieldTuple := octosql.MakeTuple(fieldValues)
	valueTuple := octosql.MakeTuple(values)

	return octosql.MakeTuple([]octosql.Value{fieldTuple, valueTuple}), nil
}
