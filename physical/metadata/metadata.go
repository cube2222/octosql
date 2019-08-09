package metadata

type Cardinality string

const (
	BoundedFitsInLocalStorage      Cardinality = "bounded_fits_in_local_storage"
	BoundedDoesntFitInLocalStorage Cardinality = "bounded_doesnt_fit_in_local_storage"
	Unbounded                      Cardinality = "unbounded"
)

var ordering = map[Cardinality]int{
	BoundedFitsInLocalStorage:      1,
	BoundedDoesntFitInLocalStorage: 2,
	Unbounded:                      3,
}

func CombineCardinalities(cardinalities ...Cardinality) Cardinality {
	max := cardinalities[0]
	for _, cardinality := range cardinalities {
		if ordering[cardinality] > ordering[max] {
			max = cardinality
		}
	}

	return max
}

type NodeMetadata struct {
	cardinality Cardinality
}

func (meta *NodeMetadata) Cardinality() Cardinality {
	return meta.cardinality
}

func NewNodeMetadata(cardinality Cardinality) *NodeMetadata {
	return &NodeMetadata{
		cardinality: cardinality,
	}
}
