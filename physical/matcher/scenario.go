package matcher

import "github.com/cube2222/octosql/physical"

type Scenario struct {
	matcher     NodeMatcher
	reassembler func(match *Match) physical.Node
}

// example, joining two subsequent filters into one.
var _ = Scenario{
	matcher: MatchNode("parent").MatchFilter().MatchFormula(MatchFormula("parent_formula")).
		MatchSource(
			MatchNode("child").MatchFilter().MatchFormula(MatchFormula("child_formula")).
				MatchSource(MatchNode("child_source")),
		),
	reassembler: func(match *Match) physical.Node {
		return &physical.Filter{
			Formula: physical.NewAnd(match.formulas["parent_formula"], match.formulas["child_formula"]),
			Source:  match.nodes["child_source"],
		}
	},
}
