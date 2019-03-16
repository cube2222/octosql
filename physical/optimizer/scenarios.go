package optimizer

import (
	"github.com/cube2222/octosql/physical"
	. "github.com/cube2222/octosql/physical/matcher" // Makes the scenarios more readable.
)

var scenarios = []Scenario{
	{
		Name:        "merge requalifiers",
		Description: "Merges two subsequent requalifiers into one.",
		CandidateMatcher: &RequalifierMatcher{
			Qualifier: &AnyStringMatcher{
				Name: "qualifier",
			},
			Source: &RequalifierMatcher{
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
	},
	{
		Name:        "merge filters",
		Description: "Merges two subsequent filters into one.",
		CandidateMatcher: &FilterMatcher{
			Formula: &AnyFormulaMatcher{
				Name: "parent_formula",
			},
			Source: &FilterMatcher{
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
	},
	{
		Name:        "merge data source with requalifier",
		Description: "Changes the data sources alias to the new qualifier in the requalifier.",
		CandidateMatcher: &RequalifierMatcher{
			Qualifier: &AnyStringMatcher{
				Name: "qualifier",
			},
			Source: &DataSourceBuilderMatcher{
				Name: "data_source_builder",
			},
		},
		Reassembler: func(match *Match) physical.Node {
			dataSourceBuilder := match.Nodes["data_source_builder"].(*physical.DataSourceBuilder)

			return &physical.DataSourceBuilder{
				Executor:         dataSourceBuilder.Executor,
				PrimaryKeys:      dataSourceBuilder.PrimaryKeys,
				AvailableFilters: dataSourceBuilder.AvailableFilters,
				Filter:           dataSourceBuilder.Filter,
				Alias:            match.Strings["qualifier"],
			}
		},
	},
}

var _ = Scenario{
	Name:        "merge data source with filter",
	Description: "Changes the data sources filter to contain the filters formula, if the data source supports it.",
	CandidateMatcher: &FilterMatcher{
		Formula: &AnyFormulaMatcher{
			Name: "parent_filter",
		},
		Source: &DataSourceBuilderMatcher{
			Name: "data_source_builder",
		},
	},
	CandidateApprover: func(match *Match) bool {
		panic("implement me")
		// return CheckIfContainsOnlyPrimaryKeys(match.Nodes["data_source_builder"], match.Formulas["parent_filter"])
	},
	Reassembler: func(match *Match) physical.Node {
		dataSourceBuilder := match.Nodes["data_source_builder"].(*physical.DataSourceBuilder)

		return &physical.DataSourceBuilder{
			Executor:         dataSourceBuilder.Executor,
			PrimaryKeys:      dataSourceBuilder.PrimaryKeys,
			AvailableFilters: dataSourceBuilder.AvailableFilters,
			Filter:           physical.NewAnd(dataSourceBuilder.Filter, match.Formulas["parent_filter"]),
			Alias:            dataSourceBuilder.Alias,
		}
	},
}
