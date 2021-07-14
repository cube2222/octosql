package optimizer

import (
	. "github.com/cube2222/octosql/physical"
)

var defaultOptimizationRules = []func(Node) (output Node, changed bool){
	PushDownFilterUnderRequalifier,
	PushDownFilterPredicatesToDatasource,
	PushDownFilterPredicatesIntoJoinBranch,
	PushDownFilterPredicatesIntoStreamJoinKey,
	MergeFilters,
}

func Optimize(node Node) Node {
	// TODO: We could actually get the value of 'changed' by diffing the tree after each round of optimizations, instead of pushing that burden onto the optimization rules.
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
