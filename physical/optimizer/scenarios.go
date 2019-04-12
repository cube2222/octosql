package optimizer

import (
	"context"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	. "github.com/cube2222/octosql/physical/matcher" // Makes the scenarios more readable.
)

var DefaultScenarios = []Scenario{
	MergeRequalifiers,
	MergeFilters,
	MergeDataSourceBuilderWithRequalifier,
	MergeDataSourceBuilderWithFilter,
}

var MergeRequalifiers = Scenario{
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
}

var MergeFilters = Scenario{
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
}

var MergeDataSourceBuilderWithRequalifier = Scenario{
	Name:        "merge data source builder with requalifier",
	Description: "Changes the data source builders alias to the new qualifier in the requalifier.",
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
			Filter:           dataSourceBuilder.Filter, // TODO: fixme variable names
			Alias:            match.Strings["qualifier"],
		}
	},
}

var MergeDataSourceBuilderWithFilter = Scenario{
	Name:        "merge data source builder with filter",
	Description: "Changes the data sources filter to contain the filters formula, if the data source supports it.",
	CandidateMatcher: &FilterMatcher{
		Formula: &AnyFormulaMatcher{
			Name: "parent_filter",
		},
		Source: &DataSourceBuilderMatcher{
			Name: "data_source_builder",
		},
	},
	CandidateApprover: func(match *Match) bool { // TODO: Pass context.
		filters := match.Formulas["parent_filter"].SplitByAnd()
		ds := match.Nodes["data_source_builder"].(*physical.DataSourceBuilder)

	filterChecker:
		for _, filter := range filters {
			predicates := filter.ExtractPredicates()
			foundAnyLocalVariables := false
			for _, predicate := range predicates {
				predicateMovable := false

				varsLeft := GetVariables(context.Background(), predicate.Left)
				varsRight := GetVariables(context.Background(), predicate.Right)
				localVarsLeft := make([]octosql.VariableName, 0)
				localVarsRight := make([]octosql.VariableName, 0)

				for i := range varsLeft {
					if varsLeft[i].Source() == ds.Alias {
						localVarsLeft = append(localVarsLeft, varsLeft[i])
					}
				}
				for i := range varsRight {
					if varsRight[i].Source() == ds.Alias {
						localVarsRight = append(localVarsRight, varsRight[i])
					}
				}

				if len(localVarsLeft) > 0 || len(localVarsRight) > 0 {
					foundAnyLocalVariables = true
				}

				if _, ok := ds.AvailableFilters[physical.Primary][predicate.Relation]; ok {
					if subset(ds.PrimaryKeys, localVarsLeft) && subset(ds.PrimaryKeys, localVarsRight) {
						predicateMovable = true
					}
				}

				if _, ok := ds.AvailableFilters[physical.Secondary][predicate.Relation]; ok {
					predicateMovable = true
				}

				if !predicateMovable {
					continue filterChecker
				}
			}
			if !foundAnyLocalVariables {
				continue
			}
			return true
		}
		return false
	},
	Reassembler: func(match *Match) physical.Node {
		filters := match.Formulas["parent_filter"].SplitByAnd()
		ds := match.Nodes["data_source_builder"].(*physical.DataSourceBuilder)

		extractable := -1

	filterChecker:
		for index, filter := range filters {
			predicates := filter.ExtractPredicates()
			foundAnyLocalVariables := false
			for _, predicate := range predicates {
				allPredicatesMovable := false

				varsLeft := GetVariables(context.Background(), predicate.Left)
				varsRight := GetVariables(context.Background(), predicate.Right)
				localVarsLeft := make([]octosql.VariableName, 0)
				localVarsRight := make([]octosql.VariableName, 0)

				for i := range varsLeft {
					if varsLeft[i].Source() == ds.Alias {
						localVarsLeft = append(localVarsLeft, varsLeft[i])
					}
				}
				for i := range varsRight {
					if varsRight[i].Source() == ds.Alias {
						localVarsRight = append(localVarsRight, varsRight[i])
					}
				}

				if len(localVarsLeft) > 0 || len(localVarsRight) > 0 {
					foundAnyLocalVariables = true
				}

				if _, ok := ds.AvailableFilters[physical.Primary][predicate.Relation]; ok {
					if subset(ds.PrimaryKeys, localVarsLeft) && subset(ds.PrimaryKeys, localVarsRight) {
						allPredicatesMovable = true
					}
				}

				if _, ok := ds.AvailableFilters[physical.Secondary][predicate.Relation]; ok {
					allPredicatesMovable = true
				}

				if !allPredicatesMovable {
					continue filterChecker
				}
			}
			if !foundAnyLocalVariables {
				continue
			}
			extractable = index
			break
		}
		dsFilter := physical.NewAnd(filters[extractable], ds.Filter)

		filters[extractable] = filters[len(filters)-1]
		filters = filters[:len(filters)-1]

		var out physical.Node = &physical.DataSourceBuilder{
			Executor:         ds.Executor,
			PrimaryKeys:      ds.PrimaryKeys,
			AvailableFilters: ds.AvailableFilters,
			Filter:           dsFilter,
			Alias:            ds.Alias,
		}

		if len(filters) > 0 {
			for len(filters) > 1 {
				filters[1] = physical.NewAnd(filters[0], filters[1])
				filters = filters[1:]
			}
			out = physical.NewFilter(filters[0], out)
		}

		return out
	},
}

func subset(set []octosql.VariableName, subset []octosql.VariableName) bool {
	for i := range subset {
		if !containsVariableName(set, subset[i]) {
			return false
		}
	}
	return true
}

func containsVariableName(vns []octosql.VariableName, vn octosql.VariableName) bool {
	for i := range vns {
		if vn.Name() == vns[i].Name() {
			return true
		}
	}
	return false
}
