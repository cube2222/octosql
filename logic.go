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

func (p *Predicate) Evaluate(record Record, primitives map[string]interface{}) bool {
	panic("implement me")
	return true
}

type Expression interface {
	ExpressionValue(record Record, primitives map[string]interface{}) interface{}
}

type Primitive struct {
	id string
}

func (p *Primitive) ExpressionValue(record Record, primitives map[string]interface{}) interface{} {
	return primitives[p.id]
}

type FieldReference struct {
	id FieldIdentifier
}

func (f *FieldReference) ExpressionValue(record Record, primitives map[string]interface{}) interface{} {
	return record.Value(f.id)
}

var expression = Predicate{
	Left: &FieldReference{
		id: "City",
	},
	Filter: Equal,
	Right: &Primitive{
		id: "city_x",
	},
}
