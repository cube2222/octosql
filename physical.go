package octosql

import "context"

type VariableName interface{}

type NodeDescription interface {
	GetVariables() []VariableName
	Initialize(ctx context.Context) Node
}

type Node interface {
	Get(variableValues map[string]interface{}) (RecordStream, error)
}

type UnionAllDescription struct {
	left, right NodeDescription
}

func (union *UnionAllDescription) GetVariables() []VariableName {
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

func (union *UnionAll) Get(variableValues map[string]interface{}) (RecordStream, error) {
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
