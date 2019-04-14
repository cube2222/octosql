package matcher

import "github.com/cube2222/octosql/physical"

// NamedExpressionListMatcher is used to match named expressions on various predicates.
type NamedExpressionListMatcher interface {
	// Match tries to match a named expression list filling the match. Returns true on success.
	Match(match *Match, expressions []physical.NamedExpression) bool
}

// AnyNamedExpressionListMatcher matches any named expression list.
type AnyNamedExpressionListMatcher struct {
	Name string
}

func (m *AnyNamedExpressionListMatcher) Match(match *Match, expressions []physical.NamedExpression) bool {
	if len(m.Name) > 0 {
		match.NamedExpressionLists[m.Name] = expressions
	}
	return true
}
