package octosql

type RelationType string

const (
	Equal    RelationType = "equal"
	NotEqual RelationType = "not_equal"
	MoreThan RelationType = "more_than"
	LessThan RelationType = "less_than"
	Like     RelationType = "like"
	In       RelationType = "in"
)

type Formula interface {
	Evaluate(record Record, primitives map[string]interface{}) bool
	Fields() map[string]struct{}
	Primitives() map[string]struct{}
}

// TODO: Implement:

type And struct {
	Left, Right Formula
}

type Or struct {
	Left, Right Formula
}

type Not struct {
	Child Formula
}

type Predicate struct {
	Left   Expression
	Filter RelationType
	Right  Expression
}

// TODO: Evaluate should probably return an error, as the RelationType may be invalid for the operand types
func (p *Predicate) Evaluate(record Record, variables map[VariableName]interface{}) bool {
	panic("implement me")
	return true
}

type Expression interface {
	ExpressionValue(record Record, variables map[VariableName]interface{}) interface{}
}

type Variable struct {
	id VariableName
}

func (p *Variable) ExpressionValue(record Record, variables map[VariableName]interface{}) interface{} {
	return variables[p.id]
}

type RecordField struct {
	id FieldIdentifier
}

func (f *RecordField) ExpressionValue(record Record, variables map[VariableName]interface{}) interface{} {
	return record.Value(f.id)
}

var expression = Predicate{
	Left: &RecordField{
		id: "City",
	},
	Filter: Equal,
	Right: &Variable{
		id: "city_x",
	},
}
