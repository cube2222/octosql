package execution

import (
	"fmt"

	"github.com/cube2222/octosql/octosql"
)

const BTreeDefaultDegree = 128

// TODO: Everywhere we iterate over columns, use col as the variable name, not i or j.
//       And row for iterating over rows. Or colIndex and rowIndex.

type Expression interface {
	Evaluate(ctx ExecutionContext) ([]octosql.Value, error)
}

type Variable struct {
	level, index int
}

func NewVariable(level, index int) *Variable {
	return &Variable{
		level: level,
		index: index,
	}
}

func (r *Variable) Evaluate(ctx ExecutionContext) ([]octosql.Value, error) {
	if r.level == 0 {
		return ctx.CurrentRecords.Values[r.index], nil
	}

	curVars := ctx.VariableContext
	for i := r.level - 1; i != 0; i-- {
		curVars = curVars.Parent
	}
	out := make([]octosql.Value, ctx.CurrentRecords.Size)
	for i := range out {
		out[i] = curVars.Values[r.index]
	}

	return out, nil
}

type Constant struct {
	value octosql.Value
}

func NewConstant(value octosql.Value) *Constant {
	return &Constant{
		value: value,
	}
}

func (c *Constant) Evaluate(ctx ExecutionContext) ([]octosql.Value, error) {
	out := ctx.SlicePool.GetSized(ctx.CurrentRecords.Size)
	for i := range out {
		out[i] = c.value
	}
	return out, nil
}

type TypeAssertion struct {
	expectedTypeIDs  []octosql.TypeID
	expectedTypeName string
	expr             Expression
}

func NewTypeAssertion(expectedTypeID []octosql.TypeID, expr Expression, expectedTypeName string) *TypeAssertion {
	return &TypeAssertion{
		expectedTypeIDs:  expectedTypeID,
		expr:             expr,
		expectedTypeName: expectedTypeName,
	}
}

func (c *TypeAssertion) Evaluate(ctx ExecutionContext) ([]octosql.Value, error) {
	values, err := c.expr.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

checkLoop:
	for i := range values {
		for j := range c.expectedTypeIDs {
			if values[i].TypeID == c.expectedTypeIDs[j] {
				continue checkLoop
			}
		}
		return nil, fmt.Errorf("invalid type: %s, expected: %s", values[i].TypeID.String(), c.expectedTypeName)
	}

	return values, nil
}

type TypeCast struct {
	targetTypeID octosql.TypeID
	expr         Expression
}

func NewTypeCast(targetTypeID octosql.TypeID, expr Expression) *TypeCast {
	return &TypeCast{
		targetTypeID: targetTypeID,
		expr:         expr,
	}
}

func (c *TypeCast) Evaluate(ctx ExecutionContext) ([]octosql.Value, error) {
	values, err := c.expr.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("couldn't evaluate cast argument: %w", err)
	}

	out := make([]octosql.Value, ctx.CurrentRecords.Size)
	for i := range values {
		if values[i].TypeID == c.targetTypeID {
			out[i] = values[i]
		} else {
			out[i] = octosql.NewNull()
		}
	}

	return out, nil
}

type FunctionCall struct {
	function         func(ExecutionContext, [][]octosql.Value) ([]octosql.Value, error)
	args             []Expression
	nullCheckIndices []int
}

func NewFunctionCall(function func(ExecutionContext, [][]octosql.Value) ([]octosql.Value, error), args []Expression, nullCheckIndices []int) *FunctionCall {
	return &FunctionCall{
		function:         function,
		args:             args,
		nullCheckIndices: nullCheckIndices,
	}
}

func (c *FunctionCall) Evaluate(ctx ExecutionContext) ([]octosql.Value, error) {
	argValues := make([][]octosql.Value, len(c.args))
	for i := range c.args {
		values, err := c.args[i].Evaluate(ctx)
		if err != nil {
			return nil, fmt.Errorf("couldn't evaluate %d argument: %w", i, err)
		}
		argValues[i] = values
	}
	outputNullBecauseOfStrictness := make([]bool, ctx.CurrentRecords.Size)
	for _, index := range c.nullCheckIndices {
		for i := range argValues[index] {
			if argValues[index][i].TypeID == octosql.TypeIDNull {
				outputNullBecauseOfStrictness[i] = true
			}
		}
	}

	values, err := c.function(ctx, argValues)
	if err != nil {
		return nil, fmt.Errorf("couldn't evaluate function: %w", err)
	}
	// TODO: Fixme
	// for i := range argValues {
	// 	ctx.SlicePool.Put(argValues[i])
	// }
	return values, nil
}

type And struct {
	args []Expression
}

func NewAnd(args []Expression) *And {
	return &And{
		args: args,
	}
}

func (c *And) Evaluate(ctx ExecutionContext) ([]octosql.Value, error) {
	panic("implement me")

	// TODO: This changes the semantics of AND and makes it not short-circuit.
	//       This could be changed by short circuiting and just evaluating a smaller number of fields in each loop by modifying the variable context passed down.

	// nullEncountered := false
	// for i := range c.args {
	// 	value, err := c.args[i].Evaluate(ctx)
	// 	if err != nil {
	// 		return octosql.ZeroValue, fmt.Errorf("couldn't evaluate %d AND argument: %w", i, err)
	// 	}
	// 	if value.TypeID == octosql.TypeIDNull {
	// 		nullEncountered = true
	// 		continue
	// 	}
	// 	if !value.Boolean {
	// 		return value, nil
	// 	}
	// }
	// if nullEncountered {
	// 	return octosql.NewNull(), nil
	// }
	//
	// return octosql.NewBoolean(true), nil
}

type Or struct {
	args []Expression
}

func NewOr(args []Expression) *Or {
	return &Or{
		args: args,
	}
}

func (c *Or) Evaluate(ctx ExecutionContext) ([]octosql.Value, error) {
	panic("implement me")

	// nullEncountered := false
	// for i := range c.args {
	// 	value, err := c.args[i].Evaluate(ctx)
	// 	if err != nil {
	// 		return octosql.ZeroValue, fmt.Errorf("couldn't evaluate %d OR argument: %w", i, err)
	// 	}
	// 	if value.Boolean {
	// 		return value, nil
	// 	}
	// 	if value.TypeID == octosql.TypeIDNull {
	// 		nullEncountered = true
	// 	}
	// }
	// if nullEncountered {
	// 	return octosql.NewNull(), nil
	// }
	// return octosql.NewBoolean(false), nil
}

type SingleColumnQueryExpression struct {
	source Node
}

func NewSingleColumnQueryExpression(source Node) *SingleColumnQueryExpression {
	return &SingleColumnQueryExpression{
		source: source,
	}
}

func (e *SingleColumnQueryExpression) Evaluate(ctx ExecutionContext) ([]octosql.Value, error) {
	// TODO: Handle retractions.
	// TODO: Opportunity for parallelism.
	out := make([]octosql.Value, ctx.CurrentRecords.Size)
	for i := range out {
		var values []octosql.Value
		if err := e.source.Run(
			ExecutionContext{
				Context:         ctx.Context,
				VariableContext: ctx.VariableContext.WithValues(ctx.CurrentRecords.Row(i)),
				CurrentRecords: RecordBatch{
					Size: 1, // TODO: Fix this hack.
				},
			},
			func(ctx ProduceContext, records RecordBatch) error {
				for _, retraction := range records.Retractions {
					if retraction {
						return fmt.Errorf("query expression currently can't handle retractions")
					}
				}
				values = append(values, records.Values[0]...)
				return nil
			},
			func(ctx ProduceContext, msg MetadataMessage) error { return nil },
		); err != nil {
			return nil, fmt.Errorf("couldn't run query expression: %w", err)
		}
		out[i] = octosql.NewList(values)
	}
	return out, nil
}

type MultiColumnQueryExpression struct {
	source Node
}

func NewMultiColumnQueryExpression(source Node) *MultiColumnQueryExpression {
	return &MultiColumnQueryExpression{
		source: source,
	}
}

func (e *MultiColumnQueryExpression) Evaluate(ctx ExecutionContext) ([]octosql.Value, error) {
	// TODO: Handle retractions.
	// TODO: Opportunity for parallelism.
	out := make([]octosql.Value, ctx.CurrentRecords.Size)
	for i := range out {
		var values []octosql.Value
		if err := e.source.Run(
			ExecutionContext{
				Context:         ctx.Context,
				VariableContext: ctx.VariableContext.WithValues(ctx.CurrentRecords.Row(i)),
				CurrentRecords: RecordBatch{
					Size: 1, // TODO: Fix this hack.
				},
			},
			func(ctx ProduceContext, records RecordBatch) error {
				for _, retraction := range records.Retractions {
					if retraction {
						return fmt.Errorf("query expression currently can't handle retractions")
					}
				}
				for row := 0; row < records.Size; row++ {
					values = append(values, octosql.NewStruct(records.Row(row)))
				}
				return nil
			},
			func(ctx ProduceContext, msg MetadataMessage) error { return nil },
		); err != nil {
			return nil, fmt.Errorf("couldn't run query expression: %w", err)
		}
		out[i] = octosql.NewList(values)
	}
	return out, nil
}

type LayoutMapping struct {
	Struct *struct {
		SourceIndex   []int
		SourceMapping []LayoutMapping
	}
	List *struct {
		ElementMapping LayoutMapping
	}
	Tuple *struct {
		ElementMapping []LayoutMapping
	}
}

type ObjectLayoutFixer struct {
	mappings []LayoutMapping
}

func NewObjectLayoutFixer(targetType octosql.Type, sourceTypes []octosql.Type) *ObjectLayoutFixer {
	mappings := make([]LayoutMapping, len(sourceTypes))
	for i := range sourceTypes {
		mappings[i] = calculateMapping(targetType, sourceTypes[i])
	}
	return &ObjectLayoutFixer{
		mappings: mappings,
	}
}

func (f *ObjectLayoutFixer) FixLayout(index int, value octosql.Value) octosql.Value {
	return f.fixLayout(f.mappings[index], value)
}

func (f *ObjectLayoutFixer) fixLayout(mapping LayoutMapping, value octosql.Value) octosql.Value {
	switch value.TypeID {
	case octosql.TypeIDStruct:
		out := make([]octosql.Value, len(mapping.Struct.SourceIndex))
		for i := range out {
			if mapping.Struct.SourceIndex[i] != -1 {
				out[i] = f.fixLayout(mapping.Struct.SourceMapping[i], value.Struct[mapping.Struct.SourceIndex[i]])
			}
		}
		return octosql.NewStruct(out)
	case octosql.TypeIDList:
		out := make([]octosql.Value, len(value.List))
		for i := range out {
			out[i] = f.fixLayout(mapping.List.ElementMapping, value.List[i])
		}
		return octosql.NewList(out)
	case octosql.TypeIDTuple:
		out := make([]octosql.Value, len(value.Tuple))
		for i := range out {
			out[i] = f.fixLayout(mapping.Tuple.ElementMapping[i], value.List[i])
		}
		return octosql.NewTuple(out)
	default:
		// primitive type
		return value
	}
}

func mergeMappings(m1, m2 LayoutMapping) LayoutMapping {
	out := LayoutMapping{}
	if m1.Struct != nil {
		out.Struct = m1.Struct
	}
	if m2.Struct != nil {
		out.Struct = m2.Struct
	}
	if m1.List != nil {
		out.List = m1.List
	}
	if m2.List != nil {
		out.List = m2.List
	}
	if m1.Tuple != nil {
		out.Tuple = m1.Tuple
	}
	if m2.Tuple != nil {
		out.Tuple = m2.Tuple
	}
	return out
}

func calculateMapping(targetType, sourceType octosql.Type) LayoutMapping {
	if sourceType.TypeID == octosql.TypeIDUnion {
		var out LayoutMapping
		for i := range sourceType.Union.Alternatives {
			out = mergeMappings(out, calculateMapping(targetType, sourceType.Union.Alternatives[i]))
		}
		return out
	}

	if targetType.TypeID == octosql.TypeIDUnion {
		for i := range targetType.Union.Alternatives {
			if targetType.Union.Alternatives[i].TypeID == sourceType.TypeID {
				return calculateMapping(targetType.Union.Alternatives[i], sourceType)
			}
		}
		panic("calculateMapping unreachable target Union alternative")
	}

	switch targetType.TypeID {
	case octosql.TypeIDStruct:
		sourceIndices := map[string]int{}
		for i := range sourceType.Struct.Fields {
			sourceIndices[sourceType.Struct.Fields[i].Name] = i
		}

		outIndices := make([]int, len(targetType.Struct.Fields))
		outMappings := make([]LayoutMapping, len(targetType.Struct.Fields))
		for i, field := range targetType.Struct.Fields {
			sourceIndex, ok := sourceIndices[field.Name]
			if !ok {
				outIndices[i] = -1
				continue
			}
			outIndices[i] = sourceIndex
			outMappings[i] = calculateMapping(field.Type, sourceType.Struct.Fields[sourceIndex].Type)
		}
		return LayoutMapping{
			Struct: &struct {
				SourceIndex   []int
				SourceMapping []LayoutMapping
			}{
				SourceIndex:   outIndices,
				SourceMapping: outMappings,
			},
		}

	case octosql.TypeIDList:
		if targetType.List.Element == nil || sourceType.List.Element == nil {
			return LayoutMapping{}
		}
		return LayoutMapping{
			List: &struct {
				ElementMapping LayoutMapping
			}{
				ElementMapping: calculateMapping(*targetType.List.Element, *sourceType.List.Element),
			},
		}

	case octosql.TypeIDTuple:
		mappings := make([]LayoutMapping, len(targetType.Tuple.Elements))
		for i := range mappings {
			mappings[i] = calculateMapping(targetType.Tuple.Elements[i], sourceType.Tuple.Elements[i])
		}
		return LayoutMapping{
			Tuple: &struct{ ElementMapping []LayoutMapping }{ElementMapping: mappings},
		}

	default:
		return LayoutMapping{}
	}
}

type Coalesce struct {
	args              []Expression
	objectLayoutFixer *ObjectLayoutFixer
}

func NewCoalesce(args []Expression, objectLayoutFixer *ObjectLayoutFixer) *Coalesce {
	return &Coalesce{
		args:              args,
		objectLayoutFixer: objectLayoutFixer,
	}
}

func (c *Coalesce) Evaluate(ctx ExecutionContext) ([]octosql.Value, error) {
	panic("implement me")
	// for i := range c.args {
	// 	value, err := c.args[i].Evaluate(ctx)
	// 	if err != nil {
	// 		return octosql.ZeroValue, fmt.Errorf("couldn't evaluate %d OR argument: %w", i, err)
	// 	}
	// 	if value.TypeID != octosql.TypeIDNull {
	// 		return c.objectLayoutFixer.FixLayout(i, value), nil
	// 	}
	// }
	// return octosql.NewNull(), nil
}

type Tuple struct {
	args []Expression
}

func NewTuple(args []Expression) *Tuple {
	return &Tuple{
		args: args,
	}
}

func (c *Tuple) Evaluate(ctx ExecutionContext) ([]octosql.Value, error) {
	values := make([][]octosql.Value, len(c.args))
	for i := range c.args {
		curValues, err := c.args[i].Evaluate(ctx)
		if err != nil {
			return nil, fmt.Errorf("couldn't evaluate %d tuple argument: %w", i, err)
		}
		values[i] = curValues
	}
	out := make([]octosql.Value, ctx.CurrentRecords.Size)
	for i := range out {
		row := make([]octosql.Value, len(c.args))
		for j := range row {
			row[j] = values[j][i]
		}
		out[i] = octosql.NewTuple(row)
	}
	return out, nil
}

type ObjectFieldAccess struct {
	object     Expression
	fieldIndex int
}

func NewObjectFieldAccess(object Expression, fieldIndex int) *ObjectFieldAccess {
	return &ObjectFieldAccess{
		object:     object,
		fieldIndex: fieldIndex,
	}
}

func (c *ObjectFieldAccess) Evaluate(ctx ExecutionContext) ([]octosql.Value, error) {
	objects, err := c.object.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("couldn't evaluate object field access object: %w", err)
	}
	out := make([]octosql.Value, ctx.CurrentRecords.Size)
	for i := range out {
		if objects[i].TypeID == octosql.TypeIDNull {
			out[i] = octosql.NewNull()
			continue
		}
		out[i] = objects[i].Struct[c.fieldIndex]
	}

	return out, nil
}

// TODO: sys.undo should create an expression which reads the current retraction status.
