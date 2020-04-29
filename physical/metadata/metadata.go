package metadata

import "github.com/cube2222/octosql"

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
	cardinality    Cardinality
	eventTimeField octosql.VariableName
	namespace      *Namespace
}

func (meta *NodeMetadata) Cardinality() Cardinality {
	return meta.cardinality
}

func (meta *NodeMetadata) EventTimeField() octosql.VariableName {
	return meta.eventTimeField
}

func (meta *NodeMetadata) Namespace() *Namespace {
	return meta.namespace
}

func NewNodeMetadata(cardinality Cardinality, eventTimeField octosql.VariableName, namespace *Namespace) *NodeMetadata {
	return &NodeMetadata{
		cardinality:    cardinality,
		eventTimeField: eventTimeField,
		namespace:      namespace,
	}
}

func NewNodeMetadataFromMetadata(meta *NodeMetadata) *NodeMetadata {
	return &NodeMetadata{
		cardinality:    meta.Cardinality(),
		eventTimeField: meta.EventTimeField(),
		namespace:      meta.Namespace(),
	}
}

type Namespace struct {
	prefixes []string
}

func NewNamespace(prefixes []string) *Namespace {
	return &Namespace{
		prefixes: prefixes,
	}
}

func EmptyNamespace() *Namespace {
	return NewNamespace(nil)
}

func (nm *Namespace) MergeWith(other *Namespace) {
	for _, prefix := range other.prefixes {
		nm.AddPrefix(prefix)
	}
}

func (nm *Namespace) AddPrefix(prefix string) {
	if !nm.DoesContainPrefix(prefix) {
		nm.prefixes = append(nm.prefixes, prefix)
	}
}

func (nm *Namespace) Equal(other *Namespace) bool {
	return nm.Contains(other) && other.Contains(nm)
}

func (nm *Namespace) Contains(other *Namespace) bool {
	for _, otherPrefix := range other.prefixes {
		if !nm.DoesContainPrefix(otherPrefix) {
			return false
		}
	}

	return true
}

func (nm *Namespace) DoesContainName(name octosql.VariableName) bool {
	return nm.DoesContainPrefix(name.Source())
}

func (nm *Namespace) DoesContainPrefix(prefix string) bool {
	return belongs(nm.prefixes, prefix)
}

func belongs(strings []string, element string) bool {
	for _, str := range strings {
		if str == element {
			return true
		}
	}
	return false
}
