package lib

const NoUnusedMark = true

type AggregateCount struct {
	Count int
}

func (c *AggregateCount) Value() int {
	return c.Count
}

type AggregateSumInt struct {
	Sum int
}

func (c *AggregateSumInt) Value() int {
	return c.Sum
}

type AggregateAvgFloat struct {
	Sum   float64
	Count int
}

func (c *AggregateAvgFloat) Value() float64 {
	return c.Sum / float64(c.Count)
}
