package matcher

import "github.com/cube2222/octosql/physical"

type Scenario struct {
	Name        string
	Description string
	Matcher     NodeMatcher
	Reassembler func(match *Match) physical.Node
}

// example, joining two subsequent requalifiers into one.
var _ = Scenario{
	Matcher: &RequalifierMatcher{
		Name: "parent",
		Qualifier: &AnyStringMatcher{
			Name: "qualifier",
		},
		Source: &RequalifierMatcher{
			Name: "child",
			Source: &AnyNodeMatcher{
				Name: "source",
			},
		},
	},
	Reassembler: func(match *Match) physical.Node {
		return &physical.Requalifier{
			Qualifier: match.Strings["qualifier"],
			Source:    match.Nodes["source"],
		}
	},
}

// example, joining two subsequent filters into one.
var _ = Scenario{
	Matcher: &FilterMatcher{
		Name: "parent",
		Formula: &AnyFormulaMatcher{
			Name: "parent_formula",
		},
		Source: &FilterMatcher{
			Name: "child",
			Formula: &AnyFormulaMatcher{
				Name: "child_formula",
			},
			Source: &AnyNodeMatcher{
				Name: "source",
			},
		},
	},
	Reassembler: func(match *Match) physical.Node {
		return &physical.Filter{
			Formula: physical.NewAnd(match.Formulas["parent_formula"], match.Formulas["child_formula"]),
			Source:  match.Nodes["source"],
		}
	},
}
