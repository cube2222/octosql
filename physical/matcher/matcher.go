package matcher

import "github.com/cube2222/octosql/physical"

// Najpierw matchujemy node'y, a potem mozemy juz zmatchowac formulki majac wiedze o node'ach

type Match struct {
	Nodes    map[string]physical.Node
	Formulas map[string]physical.Formula
	Strings  map[string]string
}
