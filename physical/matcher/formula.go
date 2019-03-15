package matcher

import (
	"log"

	"github.com/cube2222/octosql/physical"
)

type FormulaMatcher interface {
	Match(node physical.Formula) bool
	Replace(match *Match) physical.Formula
}

func MatchFormula(name string) *AnyFormulaMatcher {
	return &AnyFormulaMatcher{
		Name: name,
	}
}

type AnyFormulaMatcher struct {
	Name string
}

func (m *AnyFormulaMatcher) Match(node physical.Formula) bool {
	return true
}

func (m *AnyFormulaMatcher) Replace(match *Match) physical.Formula {
	node, ok := match.formulas[m.Name]
	if !ok {
		log.Panicf("Expected to find formula named %v after match, found %+v", m.Name, match.nodes)
	}
	return node
}

// ExtractAnd(MatchKeyVariablePredicate(dataSourceName))
func (m *AnyFormulaMatcher) ExtractAnd(predicate FormulaMatcher) *FilterMatcher {
	return &FilterMatcher{}
}
