package matcher

// PrimitiveMatcher is used to primitives on various predicates.
type PrimitiveMatcher interface {
	// Match tries to match a primitive filling the match. Returns true on success.
	Match(match *Match, node interface{}) bool
}

// AnyPrimitiveMatcher matches any primitive.
type AnyPrimitiveMatcher struct {
	Name string
}

func (m *AnyPrimitiveMatcher) Match(match *Match, primitive interface{}) bool {
	if len(m.Name) > 0 {
		match.Primitives[m.Name] = primitive
	}
	return true
}
