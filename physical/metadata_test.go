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
					nil,
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
							[]octosql.VariableName{"x.field1", "y.field2"},
						),
					),
				},
			},

			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				"a.event_time",
				metadata.NewNamespace(
					[]string{"a"},
					[]octosql.VariableName{"x.field1", "y.field2"},
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
							[]octosql.VariableName{"x.field1", "y.field2"},
						),
					),
				},
			},

			want: metadata.NewNodeMetadata(
				metadata.BoundedDoesntFitInLocalStorage,
				"event_time",
				metadata.NewNamespace(
					nil,
					[]octosql.VariableName{"x.field1", "y.field2"},
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
							[]octosql.VariableName{"a.event_time", "a.id"},
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
					[]octosql.VariableName{"out_event_time", "a.id_key"},
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
							[]octosql.VariableName{"a.event_time", "a.age", "b.event_time"},
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
					[]octosql.VariableName{"out_field", "a.age_sum"},
				),
			),
		},
		{
			name: "limit",
			node: &Limit{
				Source: &StubNode{
					NodeMetadata: metadata.NewNodeMetadata(
						metadata.Unbounded,
						octosql.NewVariableName("event_time_field"),
						metadata.NewNamespace(
							[]string{"a", "b"},
							[]octosql.VariableName{"x.field1", "x.field2"},
						),
					),
				},
			},
			want: metadata.NewNodeMetadata(
				metadata.BoundedFitsInLocalStorage,
				octosql.NewVariableName("event_time_field"),
				metadata.NewNamespace(
					[]string{"a", "b"},
					[]octosql.VariableName{"x.field1", "x.field2"},
				),
			),
		},
		{
			name: "offset",
			node: &Offset{
				Source: &StubNode{
					NodeMetadata: metadata.NewNodeMetadata(
						metadata.BoundedDoesntFitInLocalStorage,
						octosql.NewVariableName("event_time_field"),
						metadata.NewNamespace(
							[]string{"a", "b"},
							[]octosql.VariableName{"x.field1", "x.field2"},
						),
					),
				},
			},
			want: metadata.NewNodeMetadata(
				metadata.BoundedDoesntFitInLocalStorage,
				octosql.NewVariableName("event_time_field"),
				metadata.NewNamespace(
					[]string{"a", "b"},
					[]octosql.VariableName{"x.field1", "x.field2"},
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
							[]octosql.VariableName{"a.field1", "b.field2", "c.field3", "field4"},
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
					[]octosql.VariableName{"q.field1", "q.field2", "q.field3", "q.field4"},
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
							[]octosql.VariableName{"source.x", "id"},
						),
					),
				},
				Joined: &StubNode{
					NodeMetadata: metadata.NewNodeMetadata(
						metadata.BoundedFitsInLocalStorage,
						octosql.NewVariableName("joined_event_time"),
						metadata.NewNamespace(
							[]string{"c", "d", "e"},
							[]octosql.VariableName{"joined.y", "id"},
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
					[]octosql.VariableName{"source.x", "joined.y", "id"},
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
							[]octosql.VariableName{"source.x", "id"},
						),
					),
				},
				joined: &StubNode{
					NodeMetadata: metadata.NewNodeMetadata(
						metadata.BoundedFitsInLocalStorage,
						octosql.NewVariableName("joined_event_time"),
						metadata.NewNamespace(
							[]string{"c", "d", "e"},
							[]octosql.VariableName{"joined.y", "id"},
						),
					),
				},
			},
			want: metadata.NewNodeMetadata(
				metadata.BoundedDoesntFitInLocalStorage,
				octosql.NewVariableName("source_event_time"),
				metadata.NewNamespace(
					[]string{"a", "b", "c", "d", "e"},
					[]octosql.VariableName{"source.x", "joined.y", "id"},
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
