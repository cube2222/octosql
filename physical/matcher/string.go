package matcher

type StringMatcher interface {
	Match(match *Match, text string) bool
}

type AnyStringMatcher struct {
	Name string
}

func (m *AnyStringMatcher) Match(match *Match, text string) bool {
	match.Strings[m.Name] = text
	return true
}
