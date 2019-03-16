package matcher

import (
	"github.com/cube2222/octosql/physical"
)

type NodeMatcher interface {
	Match(match *Match, node physical.Node) bool
}

func MatchNode(name string) *AnyNodeMatcher {
	return &AnyNodeMatcher{
		Name: name,
	}
}

type AnyNodeMatcher struct {
	Name string
}

func (m *AnyNodeMatcher) Match(match *Match, node physical.Node) bool {
	if len(m.Name) > 0 {
		match.Nodes[m.Name] = node
	}
	return true
}

type RequalifierMatcher struct {
	Name      string
	Qualifier StringMatcher
	Source    NodeMatcher
}

func (m *RequalifierMatcher) Match(match *Match, node physical.Node) bool {
	requalifier, ok := node.(*physical.Requalifier)
	if !ok {
		return false
	}
	if m.Qualifier != nil {
		matched := m.Qualifier.Match(match, requalifier.Qualifier)
		if !matched {
			return false
		}
	}
	if m.Source != nil {
		matched := m.Source.Match(match, requalifier.Source)
		if !matched {
			return false
		}
	}
	if len(m.Name) > 0 {
		match.Nodes[m.Name] = node
	}
	return true
}

type FilterMatcher struct {
	Name    string
	Formula FormulaMatcher
	Source  NodeMatcher
}

func (m *FilterMatcher) Match(match *Match, node physical.Node) bool {
	filter, ok := node.(*physical.Filter)
	if !ok {
		return false
	}
	if m.Formula != nil {
		matched := m.Formula.Match(match, filter.Formula)
		if !matched {
			return false
		}
	}
	if m.Source != nil {
		matched := m.Source.Match(match, filter.Source)
		if !matched {
			return false
		}
	}
	if len(m.Name) > 0 {
		match.Nodes[m.Name] = node
	}
	return true
}

type DataSourceBuilderMatcher struct {
	Name    string
	Formula FormulaMatcher
	Alias   StringMatcher
}

func (m *DataSourceBuilderMatcher) Match(match *Match, node physical.Node) bool {
	dsb, ok := node.(*physical.DataSourceBuilder)
	if !ok {
		return false
	}
	if m.Formula != nil {
		matched := m.Formula.Match(match, dsb.Filter)
		if !matched {
			return false
		}
	}
	if m.Alias != nil {
		matched := m.Alias.Match(match, dsb.Alias)
		if !matched {
			return false
		}
	}
	if len(m.Name) > 0 {
		match.Nodes[m.Name] = node
	}
	return true
}
