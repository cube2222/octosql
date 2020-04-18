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
	names    []string
}

func NewNamespace(prefixes, names []string) *Namespace {
	return &Namespace{
		prefixes: prefixes,
		names:    names,
	}
}

func EmptyNamespace() *Namespace {
	return NewNamespace(make([]string, 0), make([]string, 0))
}

func (nm *Namespace) AddPrefix(prefix string) {
	nm.prefixes = append(nm.prefixes, prefix)
}

func (nm *Namespace) AddName(name string) {
	nm.names = append(nm.names, name)
}

func (nm *Namespace) MergeWith(other *Namespace) {
	for _, prefix := range other.prefixes {
		nm.AddPrefix(prefix)
	}

	for _, name := range other.names {
		nm.AddName(name)
	}
}

func (nm *Namespace) MergeWithVariables(variables octosql.Variables) {
	for name := range variables {
		nm.AddName(name.String())
	}
}

func (nm *Namespace) DoesContainPrefix(prefix string) bool {
	return belongs(nm.prefixes, prefix)
}

func (nm *Namespace) DoesContainName(name octosql.VariableName) bool {
	if nm.DoesContainPrefix(name.Source()) {
		return true
	}

	if belongs(nm.names, name.String()) {
		return true
	}
	return false
}

func (nm *Namespace) Contains(other *Namespace) bool {
	for _, otherPrefix := range other.prefixes {
		if !belongs(nm.prefixes, otherPrefix) {
			return false
		}
	}

	for _, otherName := range other.names {
		if !nm.DoesContainName(octosql.NewVariableName(otherName)) {
			return false
		}
	}

	return true
}

func belongs(strings []string, element string) bool {
	for _, str := range strings {
		if str == element {
			return true
		}
	}
	return false
}
