package execution

import (
	"fmt"

	"github.com/cube2222/octosql/octosql"
)

const BTreeDefaultDegree = 12

type Expression interface {
	Evaluate(ctx ExecutionContext) (octosql.Value, error)
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

func (r *Variable) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	curVars := ctx.VariableContext
	for i := r.level; i != 0; i-- {
		curVars = curVars.Parent
	}
	return curVars.Values[r.index], nil
}

type Constant struct {
	value octosql.Value
}

func NewConstant(value octosql.Value) *Constant {
	return &Constant{
		value: value,
	}
}

func (c *Constant) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	return c.value, nil
}

type TypeAssertion struct {
	expected octosql.Type
	expr     Expression
}

func NewTypeAssertion(expected octosql.Type, expr Expression) *TypeAssertion {
	return &TypeAssertion{
		expected: expected,
		expr:     expr,
	}
}

func (c *TypeAssertion) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	value, err := c.expr.Evaluate(ctx)
	if err != nil {
		return octosql.ZeroValue, err
	}

	if c.expected.TypeID == octosql.TypeIDStruct {
		return octosql.ZeroValue, fmt.Errorf("type assertions for structures aren't yet supported")
	}
	if value.Type().Is(c.expected) != octosql.TypeRelationIs {
		return octosql.ZeroValue, fmt.Errorf("invalid type: %s, expected: %s", value.Type(), c.expected)
	}

	return value, nil
}

type Cast struct {
	targetType octosql.Type
	expr       Expression
}

func NewCast(targetType octosql.Type, expr Expression) *Cast {
	return &Cast{
		targetType: targetType,
		expr:       expr,
	}
}

func (c *Cast) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	value, err := c.expr.Evaluate(ctx)
	if err != nil {
		return octosql.ZeroValue, fmt.Errorf("couldn't evaluate cast argument: %w", err)
	}

	if c.targetType.TypeID == octosql.TypeIDStruct {
		return octosql.ZeroValue, fmt.Errorf("type assertions for structures aren't yet supported")
	}
	if value.Type().Is(c.targetType) != octosql.TypeRelationIs {
		return octosql.NewNull(), nil
	}

	return value, nil
}

type FunctionCall struct {
	function         func([]octosql.Value) (octosql.Value, error)
	args             []Expression
	nullCheckIndices []int
}

func NewFunctionCall(function func([]octosql.Value) (octosql.Value, error), args []Expression, nullCheckIndices []int) *FunctionCall {
	return &FunctionCall{
		function:         function,
		args:             args,
		nullCheckIndices: nullCheckIndices,
	}
}

func (c *FunctionCall) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	argValues := make([]octosql.Value, len(c.args))
	for i := range c.args {
		value, err := c.args[i].Evaluate(ctx)
		if err != nil {
			return octosql.ZeroValue, fmt.Errorf("couldn't evaluate %d argument: %w", i, err)
		}
		argValues[i] = value
	}
	for _, index := range c.nullCheckIndices {
		if argValues[index].TypeID == octosql.TypeIDNull {
			return octosql.NewNull(), nil
		}
	}

	value, err := c.function(argValues)
	if err != nil {
		return octosql.ZeroValue, fmt.Errorf("couldn't evaluate function: %w", err)
	}
	return value, nil
}

type And struct {
	args []Expression
}

func NewAnd(args []Expression) *And {
	return &And{
		args: args,
	}
}

func (c *And) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	nullEncountered := false
	for i := range c.args {
		value, err := c.args[i].Evaluate(ctx)
		if err != nil {
			return octosql.ZeroValue, fmt.Errorf("couldn't evaluate %d AND argument: %w", i, err)
		}
		if value.TypeID == octosql.TypeIDNull {
			nullEncountered = true
			continue
		}
		if !value.Boolean {
			return value, nil
		}
	}
	if nullEncountered {
		return octosql.NewNull(), nil
	}

	return octosql.NewBoolean(true), nil
}

type Or struct {
	args []Expression
}

func NewOr(args []Expression) *Or {
	return &Or{
		args: args,
	}
}

func (c *Or) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	nullEncountered := false
	for i := range c.args {
		value, err := c.args[i].Evaluate(ctx)
		if err != nil {
			return octosql.ZeroValue, fmt.Errorf("couldn't evaluate %d OR argument: %w", i, err)
		}
		if value.Boolean {
			return value, nil
		}
		if value.TypeID == octosql.TypeIDNull {
			nullEncountered = true
		}
	}
	if nullEncountered {
		return octosql.NewNull(), nil
	}
	return octosql.NewBoolean(false), nil
}

type SingleColumnQueryExpression struct {
	source Node
}

func NewSingleColumnQueryExpression(source Node) *SingleColumnQueryExpression {
	return &SingleColumnQueryExpression{
		source: source,
	}
}

func (e *SingleColumnQueryExpression) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	// TODO: Handle retractions.
	var values []octosql.Value
	e.source.Run(
		ctx,
		func(ctx ProduceContext, record Record) error {
			if record.Retraction {
				return fmt.Errorf("query expression currently can't handle retractions")
			}
			values = append(values, record.Values[0])
			return nil
		},
		func(ctx ProduceContext, msg MetadataMessage) error { return nil },
	)
	return octosql.NewList(values), nil
}

type MultiColumnQueryExpression struct {
	source Node
}

func NewMultiColumnQueryExpression(source Node) *MultiColumnQueryExpression {
	return &MultiColumnQueryExpression{
		source: source,
	}
}

func (e *MultiColumnQueryExpression) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	// TODO: Handle retractions.
	var values []octosql.Value
	e.source.Run(
		ctx,
		func(ctx ProduceContext, record Record) error {
			if record.Retraction {
				return fmt.Errorf("query expression currently can't handle retractions")
			}
			values = append(values, octosql.NewStruct(record.Values))
			return nil
		},
		func(ctx ProduceContext, msg MetadataMessage) error { return nil },
	)
	return octosql.NewList(values), nil
}

// TODO: Unit tests for layout fixer.
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
	// TODO: Check if mapping is just the identity mapping. In that case we can omit the translation and set that part to LayoutMapping{} to be copied directly.
}

func calculateMapping(targetType, sourceType octosql.Type) LayoutMapping {
	// TODO: Fixme, For now assuming type unions don't exist.
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

func (c *Coalesce) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	for i := range c.args {
		value, err := c.args[i].Evaluate(ctx)
		if err != nil {
			return octosql.ZeroValue, fmt.Errorf("couldn't evaluate %d OR argument: %w", i, err)
		}
		if value.TypeID != octosql.TypeIDNull {
			return c.objectLayoutFixer.FixLayout(i, value), nil
		}
	}
	return octosql.NewNull(), nil
}

type Tuple struct {
	args []Expression
}

func NewTuple(args []Expression) *Tuple {
	return &Tuple{
		args: args,
	}
}

func (c *Tuple) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	values := make([]octosql.Value, len(c.args))
	for i := range c.args {
		value, err := c.args[i].Evaluate(ctx)
		if err != nil {
			return octosql.ZeroValue, fmt.Errorf("couldn't evaluate %d tuple argument: %w", i, err)
		}
		values[i] = value
	}
	return octosql.NewTuple(values), nil
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

func (c *ObjectFieldAccess) Evaluate(ctx ExecutionContext) (octosql.Value, error) {
	object, err := c.object.Evaluate(ctx)
	if err != nil {
		return octosql.ZeroValue, fmt.Errorf("couldn't evaluate object field access object: %w", err)
	}

	return object.Struct[c.fieldIndex], nil
}

// TODO: sys.undo should create an expression which reads the current retraction status.
