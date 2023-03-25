package wasm

import (
	"context"

	"github.com/cube2222/octosql/physical"
)

func Materialize(ctx context.Context, node physical.Node, env physical.Environment) (Node, error) {
	switch node.NodeType {
	case physical.NodeTypeTableValuedFunction:
		switch node.TableValuedFunction.Name {
		case "range":
			// TODO: Support expressions as arguments of the range node.
			return &Range{
				Start: int32(node.TableValuedFunction.Arguments["start"].Expression.Expression.Constant.Value.Int),
				End:   int32(node.TableValuedFunction.Arguments["end"].Expression.Expression.Constant.Value.Int),
			}, nil
		default:
			panic("invalid table valued function")
		}
	default:
		panic("invalid node type")
	}
}
