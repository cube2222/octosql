package physical

import (
	"fmt"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical/metadata"
)

// This file has to be here and not in the metadata package,
// because it uses both metadata and physical, and physical already
// imports metadata, so we don't want to create a cycle.

func TestNamespace(t *testing.T) {
	tests := []struct {
		name string
		node Node
		want *metadata.NodeMetadata
	}{
		{
			name: "datasource",
			node: &DataSourceBuilder{
				Name:        "source",
				Alias:       "x",
				Partition:   0,
				Cardinality: metadata.BoundedFitsInLocalStorage,
			},
			want: metadata.NewNodeMetadata(
				metadata.BoundedFitsInLocalStorage,
				octosql.NewVariableName(""),
				metadata.NewNamespace(
					[]string{"x"},
				),
			),
		},
		{
			name: "distinct",
			node: &Distinct{
				Source: &StubNode{
					NodeMetadata: metadata.NewNodeMetadata(
						metadata.Unbounded,
						"a.event_time",
						metadata.NewNamespace(
							[]string{"a"},
						),
					),
				},
			},

			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				"a.event_time",
				metadata.NewNamespace(
					[]string{"a"},
				),
			),
		},
		{
			name: "orderby",
			node: &OrderBy{
				Source: &StubNode{
					NodeMetadata: metadata.NewNodeMetadata(
						metadata.Unbounded,
						"a.event_time",
						metadata.NewNamespace(
							[]string{"a"},
						),
					),
				},
			},

			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				"a.event_time",
				metadata.NewNamespace(
					[]string{"a"},
				),
			),
		},
		{
			name: "filter",
			node: &Filter{
				Source: &StubNode{
					NodeMetadata: metadata.NewNodeMetadata(
						metadata.BoundedDoesntFitInLocalStorage,
						"event_time",
						metadata.NewNamespace(
							nil,
						),
					),
				},
			},

			want: metadata.NewNodeMetadata(
				metadata.BoundedDoesntFitInLocalStorage,
				"event_time",
				metadata.NewNamespace(
					nil,
				),
			),
		},
		{
			name: "groupby - unbounded with event time",
			node: &GroupBy{
				Source: &StubNode{
					NodeMetadata: metadata.NewNodeMetadata(
						metadata.Unbounded,
						"a.event_time",
						metadata.NewNamespace(
							nil,
						),
					),
				},
				Fields:     []octosql.VariableName{"a.event_time", "a.id"},
				Key:        []Expression{NewVariable("a.event_time"), NewVariable("a.id")},
				Aggregates: []Aggregate{Key, Key},
				As:         []octosql.VariableName{"out_event_time", ""},
			},

			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				"out_event_time",
				metadata.NewNamespace(
					nil,
				),
			),
		},
		{
			name: "groupby - bounded doesnt fit, no event time",
			node: &GroupBy{
				Source: &StubNode{
					NodeMetadata: metadata.NewNodeMetadata(
						metadata.BoundedDoesntFitInLocalStorage,
						"a.event_time",
						metadata.NewNamespace(
							nil,
						),
					),
				},
				Fields:     []octosql.VariableName{"b.event_time", "a.age"},
				Key:        []Expression{NewVariable("a.event_time"), NewVariable("b.age")},
				Aggregates: []Aggregate{Key, Sum},
				As:         []octosql.VariableName{"out_field", ""},
			},

			want: metadata.NewNodeMetadata(
				metadata.BoundedFitsInLocalStorage,
				"",
				metadata.NewNamespace(
					nil,
				),
			),
		},
		{
			name: "requalifier",
			node: &Requalifier{
				Source: &StubNode{
					NodeMetadata: metadata.NewNodeMetadata(
						metadata.Unbounded,
						octosql.NewVariableName("event_field"),
						metadata.NewNamespace(
							[]string{"a", "b", "c"},
						),
					),
				},
				Qualifier: "q",
			},
			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName("q.event_field"),
				metadata.NewNamespace(
					[]string{"q"},
				),
			),
		},
		{
			name: "stream join",
			node: &StreamJoin{
				Source: &StubNode{
					NodeMetadata: metadata.NewNodeMetadata(
						metadata.BoundedDoesntFitInLocalStorage,
						octosql.NewVariableName("source_event_time"),
						metadata.NewNamespace(
							[]string{"a", "b", "c"},
						),
					),
				},
				Joined: &StubNode{
					NodeMetadata: metadata.NewNodeMetadata(
						metadata.BoundedFitsInLocalStorage,
						octosql.NewVariableName("joined_event_time"),
						metadata.NewNamespace(
							[]string{"c", "d", "e"},
						),
					),
				},
				EventTimeField: "source_event_time",
			},
			want: metadata.NewNodeMetadata(
				metadata.BoundedDoesntFitInLocalStorage,
				octosql.NewVariableName("source_event_time"),
				metadata.NewNamespace(
					[]string{"a", "b", "c", "d", "e"},
				),
			),
		},
		{
			name: "lookup join",
			node: &LookupJoin{
				source: &StubNode{
					NodeMetadata: metadata.NewNodeMetadata(
						metadata.BoundedDoesntFitInLocalStorage,
						octosql.NewVariableName("source_event_time"),
						metadata.NewNamespace(
							[]string{"a", "b", "c"},
						),
					),
				},
				joined: &StubNode{
					NodeMetadata: metadata.NewNodeMetadata(
						metadata.BoundedFitsInLocalStorage,
						octosql.NewVariableName("joined_event_time"),
						metadata.NewNamespace(
							[]string{"c", "d", "e"},
						),
					),
				},
			},
			want: metadata.NewNodeMetadata(
				metadata.BoundedDoesntFitInLocalStorage,
				octosql.NewVariableName("source_event_time"),
				metadata.NewNamespace(
					[]string{"a", "b", "c", "d", "e"},
				),
			),
		},
		{
			name: "map test 1 - unqualified star",
			node: &Map{
				Expressions: []NamedExpression{NewStarExpression("")},
				Source: &StubNode{
					NodeMetadata: metadata.NewNodeMetadata(
						metadata.Unbounded,
						"",
						metadata.NewNamespace(
							[]string{"x", "y", "z"},
						),
					),
				},
				Keep: false,
			},
			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				"",
				metadata.NewNamespace(
					[]string{"x", "y", "z"},
				),
			),
		},
		{
			name: "map test 2 - keep = true and qualified star no match",
			node: &Map{
				Expressions: []NamedExpression{NewStarExpression("q")},
				Source: &StubNode{
					NodeMetadata: metadata.NewNodeMetadata(
						metadata.Unbounded,
						"",
						metadata.NewNamespace(
							[]string{"x", "y", "z"},
						),
					),
				},
				Keep: true,
			},
			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				"",
				metadata.NewNamespace(
					[]string{"x", "y", "z"}, // don't add q because it's not in source metadata
				),
			),
		},
		{
			name: "map test 3 - keep = false and qualified star match",
			node: &Map{
				Expressions: []NamedExpression{NewStarExpression("x"), NewStarExpression("y")},
				Source: &StubNode{
					NodeMetadata: metadata.NewNodeMetadata(
						metadata.Unbounded,
						"",
						metadata.NewNamespace(
							[]string{"x", "y", "z"},
						),
					),
				},
				Keep: false,
			},
			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				"",
				metadata.NewNamespace(
					[]string{"x", "y"},
				),
			),
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			got := tt.node.Metadata()

			areNamespacesEqual := got.Namespace().Equal(tt.want.Namespace())

			if got.EventTimeField() != tt.want.EventTimeField() || got.Cardinality() != tt.want.Cardinality() || !areNamespacesEqual {
				t.Errorf("Metadata() = %v, want %v", got, tt.want)
			}
		})
	}
}
