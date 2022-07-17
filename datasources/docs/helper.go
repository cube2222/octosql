package docs

import (
	"golang.org/x/exp/slices"
)

type NameAndDetails[V any] struct {
	Name    string
	Details V
}

func sortedMapNameAndDetails[V any](m map[string]V) []NameAndDetails[V] {
	pairs := make([]NameAndDetails[V], 0, len(m))
	for k, v := range m {
		pairs = append(pairs, NameAndDetails[V]{k, v})
	}
	slices.SortFunc(pairs, func(a, b NameAndDetails[V]) bool {
		return a.Name < b.Name
	})
	return pairs
}
