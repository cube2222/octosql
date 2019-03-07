package octosql

// TODO: Probably should be interface
type RelationType string

const (
	Equal    RelationType = "equal"
	NotEqual RelationType = "not_equal"
	MoreThan RelationType = "more_than"
	LessThan RelationType = "less_than"
	Like     RelationType = "like"
	In       RelationType = "in"
)

func (RelationType) Apply(left, right interface{}) (bool, error) {
	panic("not implemented yet")
}

type Formula interface {
	Evaluate(record Record, primitives map[string]interface{}) (bool, error)
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

func (p *Predicate) Evaluate(record Record, variables map[VariableName]interface{}) (bool, error) {
	return p.Filter.Apply(p.Left, p.Right)
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
