package matcher

import "github.com/cube2222/octosql/physical"

// Match represents a single scenario match. It gets filled with matched objects on the way.
type Match struct {
	Formulas             map[string]physical.Formula
	NamedExpressionLists map[string][]physical.NamedExpression
	Nodes                map[string]physical.Node
	Primitives           map[string]interface{}
	Strings              map[string]string
}

// NewMatch creates an empty match.
func NewMatch() *Match {
	return &Match{
		Formulas:             make(map[string]physical.Formula),
		NamedExpressionLists: make(map[string][]physical.NamedExpression),
		Nodes:                make(map[string]physical.Node),
		Primitives:           make(map[string]interface{}),
		Strings:              make(map[string]string),
	}
}
