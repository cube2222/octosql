package optimizer

import (
	. "github.com/cube2222/octosql/physical"
)

var defaultOptimizationRules = []func(Node) (output Node, changed bool){
	PushDownFilterUnderRequalifier,
	PushDownPredicatesToDatasource,
	MergeFilters,
}

func Optimize(node Node) Node {
	changed := true
	i := 0
	for changed {
		i++
		changed = false
		for _, rule := range defaultOptimizationRules {
			output, curChanged := rule(node)
			if curChanged {
				changed = true
				node = output
			}
		}
	}
	return node
}
