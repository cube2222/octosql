package matcher

// StringMatcher is used to match strings on different predicates.
type StringMatcher interface {
	// Match tries to match a string filling the match. Returns true on success.
	Match(match *Match, text string) bool
}

// AnyStringMatcher matches any string.
type AnyStringMatcher struct {
	Name string
}

func (m *AnyStringMatcher) Match(match *Match, text string) bool {
	if len(m.Name) > 0 {
		match.Strings[m.Name] = text
	}
	return true
}
