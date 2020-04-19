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
	names    []octosql.VariableName
}

func NewNamespace(prefixes []string, names []octosql.VariableName) *Namespace {
	return &Namespace{
		prefixes: prefixes,
		names:    names,
	}
}

func (nm *Namespace) Names() []octosql.VariableName {
	return nm.names
}

func EmptyNamespace() *Namespace {
	return NewNamespace(nil, nil)
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
		nm.AddName(name)
	}
}

func (nm *Namespace) AddPrefix(prefix string) {
	if !nm.DoesContainPrefix(prefix) {
		nm.prefixes = append(nm.prefixes, prefix)
	}
}

func (nm *Namespace) AddName(name octosql.VariableName) {
	if !nm.DoesContainName(name) {
		nm.names = append(nm.names, name)
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

	for _, otherName := range other.names {
		if !nm.DoesContainName(otherName) {
			return false
		}
	}

	return true
}

func (nm *Namespace) DoesContainName(name octosql.VariableName) bool {
	if nm.DoesContainPrefix(name.Source()) {
		return true
	}

	if belongsV(nm.names, name) {
		return true
	}
	return false
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

func belongsV(vNames []octosql.VariableName, element octosql.VariableName) bool {
	for _, vName := range vNames {
		if vName == element {
			return true
		}
	}

	return false
}
