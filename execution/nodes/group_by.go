package nodes

import . "github.com/cube2222/octosql/execution"

type GroupBy struct {
	// Const (zero) or Field.
	recordTimeExpr Expression
}
