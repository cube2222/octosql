package optimizer

import (
	"context"
	"strings"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
	. "github.com/cube2222/octosql/physical/matcher" // Makes the scenarios more readable.
)

var DefaultScenarios = []Scenario{
	MergeRequalifiers,
	MergeFilters,
	MergeDataSourceBuilderWithRequalifier,
	MergeDataSourceBuilderWithFilter,
	PushFilterBelowMap,
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
			Materializer:     dataSourceBuilder.Materializer,
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
			for _, predicate := range predicates {
				pushdownable := false
				varsLeft := GetVariables(context.Background(), predicate.Left)
				varLeft, leftOk := predicate.Left.(*physical.Variable)
				varsRight := GetVariables(context.Background(), predicate.Right)
				varRight, rightOk := predicate.Right.(*physical.Variable)

				varLeftPushdownable := leftOk && varLeft.Name.Source() == ds.Alias
				varRightPushdownable := rightOk && varRight.Name.Source() == ds.Alias

				if varLeftPushdownable && varRightPushdownable {
					if _, ok := ds.AvailableFilters[physical.Secondary][predicate.Relation]; ok {
						pushdownable = true
					}
				}

				var localVarsLeftPresent, localVarsRightPresent bool

				for i := range varsLeft {
					if varsLeft[i].Source() == ds.Alias {
						localVarsLeftPresent = true
					}
				}
				for i := range varsRight {
					if varsRight[i].Source() == ds.Alias {
						localVarsRightPresent = true
					}
				}

				if varLeftPushdownable && !localVarsRightPresent {
					if _, ok := ds.AvailableFilters[physical.Primary][predicate.Relation]; ok {
						if subset(ds.PrimaryKeys, []octosql.VariableName{varLeft.Name}) {
							pushdownable = true
						}
					}

					if _, ok := ds.AvailableFilters[physical.Secondary][predicate.Relation]; ok {
						pushdownable = true
					}
				}

				if varRightPushdownable && !localVarsLeftPresent {
					if _, ok := ds.AvailableFilters[physical.Primary][predicate.Relation]; ok {
						if subset(ds.PrimaryKeys, []octosql.VariableName{varRight.Name}) {
							pushdownable = true
						}
					}

					if _, ok := ds.AvailableFilters[physical.Secondary][predicate.Relation]; ok {
						pushdownable = true
					}
				}
				if !pushdownable {
					continue filterChecker
				}
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
			for _, predicate := range predicates {
				pushdownable := false
				varsLeft := GetVariables(context.Background(), predicate.Left)
				varLeft, leftOk := predicate.Left.(*physical.Variable)
				varsRight := GetVariables(context.Background(), predicate.Right)
				varRight, rightOk := predicate.Right.(*physical.Variable)

				varLeftPushdownable := leftOk && varLeft.Name.Source() == ds.Alias
				varRightPushdownable := rightOk && varRight.Name.Source() == ds.Alias

				if varLeftPushdownable && varRightPushdownable {
					if _, ok := ds.AvailableFilters[physical.Secondary][predicate.Relation]; ok {
						pushdownable = true
					}
				}

				var localVarsLeftPresent, localVarsRightPresent bool

				for i := range varsLeft {
					if varsLeft[i].Source() == ds.Alias {
						localVarsLeftPresent = true
					}
				}
				for i := range varsRight {
					if varsRight[i].Source() == ds.Alias {
						localVarsRightPresent = true
					}
				}

				if varLeftPushdownable && !localVarsRightPresent {
					if _, ok := ds.AvailableFilters[physical.Primary][predicate.Relation]; ok {
						if subset(ds.PrimaryKeys, []octosql.VariableName{varLeft.Name}) {
							pushdownable = true
						}
					}

					if _, ok := ds.AvailableFilters[physical.Secondary][predicate.Relation]; ok {
						pushdownable = true
					}
				}

				if varRightPushdownable && !localVarsLeftPresent {
					if _, ok := ds.AvailableFilters[physical.Primary][predicate.Relation]; ok {
						if subset(ds.PrimaryKeys, []octosql.VariableName{varRight.Name}) {
							pushdownable = true
						}
					}

					if _, ok := ds.AvailableFilters[physical.Secondary][predicate.Relation]; ok {
						pushdownable = true
					}
				}
				if !pushdownable {
					continue filterChecker
				}
			}
			extractable = index
			break
		}
		dsFilter := physical.NewAnd(filters[extractable], ds.Filter)

		filters[extractable] = filters[len(filters)-1]
		filters = filters[:len(filters)-1]

		var out physical.Node = &physical.DataSourceBuilder{
			Materializer:     ds.Materializer,
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

var PushFilterBelowMap = Scenario{
	Name:        "push filter below map",
	Description: "Creates a new filter under the map containing predicates which can be checked before mapping.",
	CandidateMatcher: &FilterMatcher{
		Formula: &AnyFormulaMatcher{
			Name: "parent_filter",
		},
		Source: &MapMatcher{
			Expressions: &AnyNamedExpressionListMatcher{
				Name: "child_expressions",
			},
			Keep: &AnyPrimitiveMatcher{
				Name: "child_keep",
			},
			Source: &AnyNodeMatcher{
				Name: "child_source",
			},
		},
	},
	CandidateApprover: func(match *Match) bool { // TODO: Pass context.
		filters := match.Formulas["parent_filter"].SplitByAnd()

		for _, filter := range filters {
			predicates := filter.ExtractPredicates()
			foundNoLocalVariables := true
			for _, predicate := range predicates {
				varsLeft := GetVariables(context.Background(), predicate.Left)
				varsRight := GetVariables(context.Background(), predicate.Right)
				vars := append(varsLeft, varsRight...)

				for _, varname := range vars {
					if varname.Source() == "" && !strings.HasPrefix(varname.Name(), "const_") { //TODO: hax, fixme, physical plan should contain constants. (why get rid of useful information)
						foundNoLocalVariables = false
					}
				}
			}
			if foundNoLocalVariables {
				return true
			}
		}
		return false
	},
	Reassembler: func(match *Match) physical.Node {
		filters := match.Formulas["parent_filter"].SplitByAnd()

		extractable := -1

		for index, filter := range filters {
			predicates := filter.ExtractPredicates()
			foundNoLocalVariables := true
			for _, predicate := range predicates {
				varsLeft := GetVariables(context.Background(), predicate.Left)
				varsRight := GetVariables(context.Background(), predicate.Right)
				vars := append(varsLeft, varsRight...)

				for _, varname := range vars {
					if varname.Source() == "" && !strings.HasPrefix(varname.Name(), "const_") { //TODO: hax, fixme, physical plan should contain constants. (why get rid of useful information)
						foundNoLocalVariables = false
					}
				}
			}
			if foundNoLocalVariables {
				extractable = index
				break
			}
		}
		extractableFilter := filters[extractable]
		filters[extractable] = filters[len(filters)-1]
		filters = filters[:len(filters)-1]

		var out physical.Node = &physical.Map{
			Expressions: match.NamedExpressionLists["child_expressions"],
			Keep:        match.Primitives["child_keep"].(bool),
			Source: &physical.Filter{
				Formula: extractableFilter,
				Source:  match.Nodes["child_source"],
			},
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
