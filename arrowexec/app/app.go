package app

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/arrowexec/execution"
	"github.com/cube2222/octosql/physical"
)

func RunNode(ctx context.Context, node physical.Node, env physical.Environment) error {
	execNode, err := node.Materialize(ctx, env)
	if err != nil {
		return fmt.Errorf("couldn't materialize node: %w", err)
	}
	return execNode.Node.Run(execution.Context{
		Context: ctx,
	}, func(produceCtx execution.ProduceContext, record execution.Record) error {
		fmt.Println(record)
		return nil
	})
}
