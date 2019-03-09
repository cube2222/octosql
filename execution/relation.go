package execution

type Relation interface {
	Apply(left, right interface{}) (bool, error)
}

type Equal struct {
}

func NewEqual() Relation {
	return &Equal{}
}

func (rel *Equal) Apply(left, right interface{}) (bool, error) {
	panic("implement me")
}

type NotEqual struct {
}

func NewNotEqual() Relation {
	return &NotEqual{}
}

func (rel *NotEqual) Apply(left, right interface{}) (bool, error) {
	panic("implement me")
}

type MoreThan struct {
}

func NewMoreThan() Relation {
	return &MoreThan{}
}

func (rel *MoreThan) Apply(left, right interface{}) (bool, error) {
	panic("implement me")
}

type LessThan struct {
}

func NewLessThan() Relation {
	return &LessThan{}
}

func (rel *LessThan) Apply(left, right interface{}) (bool, error) {
	panic("implement me")
}

type Like struct {
}

func NewLike() Relation {
	return &Like{}
}

func (rel *Like) Apply(left, right interface{}) (bool, error) {
	panic("implement me")
}

type In struct {
}

func NewIn() Relation {
	return &In{}
}

func (rel *In) Apply(left, right interface{}) (bool, error) {
	panic("implement me")
}
