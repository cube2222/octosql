package execution

import (
	"context"

	"github.com/pkg/errors"
	"github.com/twmb/murmur3"

	"github.com/cube2222/octosql"
)

type ShuffleStrategyPrototype interface {
	Get(ctx context.Context, variables octosql.Variables) (ShuffleStrategy, error)
}

type KeyHashingStrategyPrototype struct {
	key []Expression
}

func NewKeyHashingStrategyPrototype(key []Expression) ShuffleStrategyPrototype {
	return &KeyHashingStrategyPrototype{
		key: key,
	}
}

func (s *KeyHashingStrategyPrototype) Get(ctx context.Context, variables octosql.Variables) (ShuffleStrategy, error) {
	return NewKeyHashingStrategy(variables, s.key), nil
}

type ConstantStrategyPrototype struct {
	partition int
}

func NewConstantStrategyPrototype(partition int) ShuffleStrategyPrototype {
	return &ConstantStrategyPrototype{
		partition: partition,
	}
}

func (s *ConstantStrategyPrototype) Get(ctx context.Context, variables octosql.Variables) (ShuffleStrategy, error) {
	return NewConstantStrategy(s.partition), nil
}

type ShuffleStrategy interface {
	// Return output partition index based on the record and output partition count.
	CalculatePartition(ctx context.Context, record *Record, outputs int) (int, error)
}

type KeyHashingStrategy struct {
	variables octosql.Variables
	key       []Expression
}

func NewKeyHashingStrategy(variables octosql.Variables, key []Expression) ShuffleStrategy {
	return &KeyHashingStrategy{
		variables: variables,
		key:       key,
	}
}

// TODO: The key should really be calculated by the preceding map. Like all group by values.
func (s *KeyHashingStrategy) CalculatePartition(ctx context.Context, record *Record, outputs int) (int, error) {
	variables, err := s.variables.MergeWith(record.AsVariables())
	if err != nil {
		return -1, errors.Wrap(err, "couldn't merge stream variables with record")
	}

	key := make([]octosql.Value, len(s.key))
	for i := range s.key {
		if _, ok := s.key[i].(*RecordExpression); ok {
			key[i], err = s.key[i].ExpressionValue(ctx, record.AsVariables())
		} else {
			key[i], err = s.key[i].ExpressionValue(ctx, variables)
		}
		if err != nil {
			return -1, errors.Wrapf(err, "couldn't evaluate key expression with index %v", i)
		}
	}

	hash := murmur3.New32()

	keyTuple := octosql.MakeTuple(key)
	_, err = hash.Write(keyTuple.MonotonicMarshal())
	if err != nil {
		return -1, errors.Wrap(err, "couldn't write to hash")
	}

	return int(hash.Sum32() % uint32(outputs)), nil
}

type ConstantStrategy struct {
	partition int
}

func NewConstantStrategy(partition int) ShuffleStrategy {
	return &ConstantStrategy{
		partition: partition,
	}
}

func (s *ConstantStrategy) CalculatePartition(ctx context.Context, record *Record, outputs int) (int, error) {
	return s.partition, nil
}
