package matcher

import "github.com/cube2222/octosql/physical"

// Najpierw matchujemy node'y, a potem mozemy juz zmatchowac formulki majac wiedze o node'ach

type Match struct {
	nodes    map[string]physical.Node
	formulas map[string]physical.Formula
}
