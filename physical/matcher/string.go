package matcher

type StringMatcher interface {
	Match(match *Match, text string) bool
}

type AnyStringMatcher struct {
	Name string
}

func (m *AnyStringMatcher) Match(match *Match, text string) bool {
	if len(m.Name) > 0 {
		match.Strings[m.Name] = text
	}
	return true
}
