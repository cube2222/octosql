package matcher

import "github.com/cube2222/octosql/physical"

// Match represents a single scenario match. It gets filled with matched objects on the way.
type Match struct {
	Nodes    map[string]physical.Node
	Formulas map[string]physical.Formula
	Strings  map[string]string
}

// NewMatch creates an empty match.
func NewMatch() *Match {
	return &Match{
		Nodes:    make(map[string]physical.Node),
		Formulas: make(map[string]physical.Formula),
		Strings:  make(map[string]string),
	}
}
