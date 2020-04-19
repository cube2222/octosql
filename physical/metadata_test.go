package physical

import (
	"fmt"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical/metadata"
)

// This file has to be here and not in the metadata package,
// because it uses both metadada and physical, and physical already
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
					metadata: metadata.NewNodeMetadata(
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
					metadata: metadata.NewNodeMetadata(
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
					metadata: metadata.NewNodeMetadata(
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
					metadata: metadata.NewNodeMetadata(
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
