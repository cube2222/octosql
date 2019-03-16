package matcher

import (
	"github.com/cube2222/octosql/physical"
)

type FormulaMatcher interface {
	Match(match *Match, formula physical.Formula) bool
}

func MatchFormula(name string) *AnyFormulaMatcher {
	return &AnyFormulaMatcher{
		Name: name,
	}
}

type AnyFormulaMatcher struct {
	Name string
}

func (m *AnyFormulaMatcher) Match(match *Match, formula physical.Formula) bool {
	match.Formulas[m.Name] = formula
	return true
}

// ExtractAnd(MatchKeyVariablePredicate(dataSourceName))
func (m *AnyFormulaMatcher) ExtractAnd(predicate FormulaMatcher) *FilterMatcher {
	return &FilterMatcher{}
}
