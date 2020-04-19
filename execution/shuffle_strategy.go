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
	keyExpression []Expression
}

func NewKeyHashingStrategyPrototype(keyExpression []Expression) ShuffleStrategyPrototype {
	return &KeyHashingStrategyPrototype{
		keyExpression: keyExpression,
	}
}

func (s *KeyHashingStrategyPrototype) Get(ctx context.Context, variables octosql.Variables) (ShuffleStrategy, error) {
	return NewKeyHashingStrategy(variables, s.keyExpression), nil
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
	variables     octosql.Variables
	keyExpression []Expression
}

func NewKeyHashingStrategy(variables octosql.Variables, keyExpression []Expression) ShuffleStrategy {
	return &KeyHashingStrategy{
		variables:     variables,
		keyExpression: keyExpression,
	}
}

// TODO: The key should really be calculated by the preceding map. Like all group by values.
func (s *KeyHashingStrategy) CalculatePartition(ctx context.Context, record *Record, outputs int) (int, error) {
	variables, err := s.variables.MergeWith(record.AsVariables())
	if err != nil {
		return -1, errors.Wrap(err, "couldn't merge stream variables with record")
	}

	key := make([]octosql.Value, len(s.keyExpression))
	for i := range s.keyExpression {
		if _, ok := s.keyExpression[i].(*RecordExpression); ok {
			key[i], err = s.keyExpression[i].ExpressionValue(ctx, record.AsVariables())
		} else {
			key[i], err = s.keyExpression[i].ExpressionValue(ctx, variables)
		}
		if err != nil {
			return -1, errors.Wrapf(err, "couldn't evaluate process key expression with index %v", i)
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
