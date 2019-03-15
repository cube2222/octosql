package matcher

import (
	"log"

	"github.com/cube2222/octosql/physical"
)

type NodeMatcher interface {
	Match(node physical.Node) bool
	Replace(match *Match) physical.Node
}

func MatchNode(name string) *AnyNodeMatcher {
	return &AnyNodeMatcher{
		Name: name,
	}
}

type AnyNodeMatcher struct {
	Name string
}

func (m *AnyNodeMatcher) Match(node physical.Node) bool {
	return true
}

func (m *AnyNodeMatcher) Replace(match *Match) physical.Node {
	node, ok := match.nodes[m.Name]
	if !ok {
		log.Panicf("Expected to find node named %v after match, found %+v", m.Name, match.nodes)
	}
	return node
}

func (m *AnyNodeMatcher) MatchFilter() *FilterMatcher {
	panic("implement me")
	return &FilterMatcher{
		Formula: &AnyFormulaMatcher{"unique"},
		Source:  &AnyNodeMatcher{"unique"},
	}
}

type FilterMatcher struct {
	Formula FormulaMatcher
	Source  NodeMatcher
}

func (m *FilterMatcher) Match(node physical.Node) bool {
	panic("implement me")
}

func (m *FilterMatcher) Replace(match *Match) physical.Node {
	return &physical.Filter{
		Formula: m.Formula.Replace(match),
		Source:  m.Source.Replace(match),
	}
}

func (m *FilterMatcher) MatchSource(matcher NodeMatcher) *FilterMatcher {
	panic("implement me")
}

func (m *FilterMatcher) MatchFormula(matcher FormulaMatcher) *FilterMatcher {
	panic("implement me")
}

func (m *AnyNodeMatcher) MatchDataSource() *DataSourceMatcher {
	return &DataSourceMatcher{}
}

type DataSourceMatcher struct {
}

func (*DataSourceMatcher) Match(node physical.Node) bool {
	panic("implement me")
}

func (*DataSourceMatcher) Replace(match *Match) physical.Node {
	panic("implement me")
}
