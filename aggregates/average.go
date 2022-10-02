package aggregates

import (
	"time"

	"github.com/cube2222/octosql/execution/nodes"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

var AverageOverloads = []physical.AggregateDescriptor{
	{
		ArgumentType: octosql.Int,
		OutputType:   octosql.Int,
		Prototype:    NewAverageIntPrototype(),
	},
	{
		ArgumentType: octosql.Float,
		OutputType:   octosql.Float,
		Prototype:    NewAverageFloatPrototype(),
	},
	{
		ArgumentType: octosql.Duration,
		OutputType:   octosql.Duration,
		Prototype:    NewAverageDurationPrototype(),
	},
}

type AverageInt struct {
	sum   SumInt
	count Count
}

func NewAverageIntPrototype() func() nodes.Aggregate {
	return func() nodes.Aggregate {
		return &AverageInt{
			sum:   SumInt{},
			count: Count{},
		}
	}
}

func (c *AverageInt) Add(retractions []bool, values []octosql.Value) bool {
	c.sum.Add(retractions, values)
	return c.count.Add(retractions, values)
}

func (c *AverageInt) Trigger() octosql.Value {
	return octosql.NewInt(c.sum.Trigger().Int / c.count.Trigger().Int)
}

type AverageFloat struct {
	sum   SumFloat
	count Count
}

func NewAverageFloatPrototype() func() nodes.Aggregate {
	return func() nodes.Aggregate {
		return &AverageFloat{
			sum:   SumFloat{},
			count: Count{},
		}
	}
}

func (c *AverageFloat) Add(retractions []bool, values []octosql.Value) bool {
	c.sum.Add(retractions, values)
	return c.count.Add(retractions, values)
}

func (c *AverageFloat) Trigger() octosql.Value {
	return octosql.NewFloat(c.sum.Trigger().Float / float64(c.count.Trigger().Int))
}

type AverageDuration struct {
	sum   SumDuration
	count Count
}

func NewAverageDurationPrototype() func() nodes.Aggregate {
	return func() nodes.Aggregate {
		return &AverageDuration{
			sum:   SumDuration{},
			count: Count{},
		}
	}
}

func (c *AverageDuration) Add(retractions []bool, values []octosql.Value) bool {
	c.sum.Add(retractions, values)
	return c.count.Add(retractions, values)
}

func (c *AverageDuration) Trigger() octosql.Value {
	return octosql.NewDuration(c.sum.Trigger().Duration / time.Duration(c.count.Trigger().Int))
}
