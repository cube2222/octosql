package aggregates

import (
	"time"

	"github.com/cube2222/octosql/execution/nodes"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

var SumOverloads = []physical.AggregateDescriptor{
	{
		ArgumentType: octosql.Int,
		OutputType:   octosql.Int,
		// Prototype:    NewSumIntPrototype(),
	},
	{
		ArgumentType: octosql.Float,
		OutputType:   octosql.Float,
		// Prototype:    NewSumFloatPrototype(),
	},
	{
		ArgumentType: octosql.Duration,
		OutputType:   octosql.Duration,
		// Prototype:    NewSumDurationPrototype(),
	},
}

type SumInt struct {
	sum int
}

func NewSumIntPrototype() func() nodes.Aggregate {
	return func() nodes.Aggregate {
		return &SumInt{
			sum: 0,
		}
	}
}

func (c *SumInt) Add(retraction bool, value octosql.Value) bool {
	if !retraction {
		c.sum += value.Int
	} else {
		c.sum -= value.Int
	}
	return c.sum == 0
}

func (c *SumInt) Trigger() octosql.Value {
	return octosql.NewInt(c.sum)
}

type SumFloat struct {
	sum float64
}

func NewSumFloatPrototype() func() nodes.Aggregate {
	return func() nodes.Aggregate {
		return &SumFloat{
			sum: 0,
		}
	}
}

func (c *SumFloat) Add(retraction bool, value octosql.Value) bool {
	if !retraction {
		c.sum += value.Float
	} else {
		c.sum -= value.Float
	}
	return c.sum == 0
}

func (c *SumFloat) Trigger() octosql.Value {
	return octosql.NewFloat(c.sum)
}

type SumDuration struct {
	sum time.Duration
}

func NewSumDurationPrototype() func() nodes.Aggregate {
	return func() nodes.Aggregate {
		return &SumDuration{
			sum: 0,
		}
	}
}

func (c *SumDuration) Add(retraction bool, value octosql.Value) bool {
	if !retraction {
		c.sum += value.Duration
	} else {
		c.sum -= value.Duration
	}
	return c.sum == 0
}

func (c *SumDuration) Trigger() octosql.Value {
	return octosql.NewDuration(c.sum)
}
