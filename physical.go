package octosql

import "context"

type Variable interface{}

type NodeDescription interface {
	GetVariables() []Variable
	Initialize(ctx context.Context) Node
}

type Node interface {
	Get(predicateValues []interface{}) (RecordStream, error)
}

type UnionAllDescription struct {
	left, right NodeDescription
}

func (union *UnionAllDescription) GetVariables() []Variable {
	return append(union.left.GetVariables(), union.right.GetVariables()...)
}

func (union *UnionAllDescription) Initialize(ctx context.Context) Node {
	return &UnionAll{
		left:  union.left.Initialize(ctx),
		right: union.right.Initialize(ctx),
	}
}

type UnionAll struct {
	left, right Node
}

func (union *UnionAll) Get(predicateValues []interface{}) (RecordStream, error) {
	panic("not implemented yet")

	/*stream, err := union.left.Get(predicateValues)
	if err != nil {
		return nil, err
	}
	streamSecond, err := union.right.Get(predicateValues)
	if err != nil {
		return nil, err
	}*/
	//return ConcatStreams(stream, streamSecond), nil
}
