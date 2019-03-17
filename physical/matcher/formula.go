package matcher

import (
	"github.com/cube2222/octosql/physical"
)

// FormulaMatcher is used to match formulas on different predicates.
type FormulaMatcher interface {
	// Match tries to match a formula filling the match. Returns true on success.
	Match(match *Match, formula physical.Formula) bool
}

// AnyFormulaMatcher matches any formula.
type AnyFormulaMatcher struct {
	Name string
}

func (m *AnyFormulaMatcher) Match(match *Match, formula physical.Formula) bool {
	if len(m.Name) > 0 {
		match.Formulas[m.Name] = formula
	}
	return true
}
