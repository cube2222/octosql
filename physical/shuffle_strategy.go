package physical

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
)

type ShuffleStrategy interface {
	Transform(ctx context.Context, transformers *Transformers) ShuffleStrategy
	Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.ShuffleStrategyPrototype, error)
	Visualize() *graph.Node
}

type KeyHashingStrategy struct {
	Key []Expression
}

func NewKeyHashingStrategy(key []Expression) ShuffleStrategy {
	return &KeyHashingStrategy{
		Key: key,
	}
}

func (s *KeyHashingStrategy) Transform(ctx context.Context, transformers *Transformers) ShuffleStrategy {
	transformedKey := make([]Expression, len(s.Key))
	for i := range s.Key {
		transformedKey[i] = s.Key[i].Transform(ctx, transformers)
	}
	var transformed ShuffleStrategy = &KeyHashingStrategy{
		Key: transformedKey,
	}
	if transformers.ShuffleStrategyT != nil {
		transformed = transformers.ShuffleStrategyT(transformed)
	}

	return transformed
}

func (s *KeyHashingStrategy) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.ShuffleStrategyPrototype, error) {
	key := make([]execution.Expression, len(s.Key))
	for i := range s.Key {
		matKey, err := s.Key[i].Materialize(ctx, matCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't materialize key part with index %d", i)
		}
		key[i] = matKey
	}

	return execution.NewKeyHashingStrategyPrototype(key), nil
}

func (s *KeyHashingStrategy) Visualize() *graph.Node {
	n := graph.NewNode("Key Hashing Strategy")

	for i, input := range s.Key {
		n.AddChild(fmt.Sprintf("key_%d", i), input.Visualize())
	}
	return n
}

type ConstantStrategy struct {
	partition int
}

func NewConstantStrategy(partition int) ShuffleStrategy {
	return &ConstantStrategy{
		partition: partition,
	}
}

func (s *ConstantStrategy) Transform(ctx context.Context, transformers *Transformers) ShuffleStrategy {
	var transformed ShuffleStrategy = &ConstantStrategy{
		partition: s.partition,
	}
	if transformers.ShuffleStrategyT != nil {
		transformed = transformers.ShuffleStrategyT(transformed)
	}

	return transformed
}

func (s *ConstantStrategy) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.ShuffleStrategyPrototype, error) {
	return execution.NewConstantStrategyPrototype(s.partition), nil
}

func (s *ConstantStrategy) Visualize() *graph.Node {
	n := graph.NewNode("Constant Strategy")

	n.AddField("partition", fmt.Sprint(s.partition))

	return n
}
