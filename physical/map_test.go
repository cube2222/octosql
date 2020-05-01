package physical

import (
	"fmt"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical/metadata"
)

func TestMap_Metadata(t *testing.T) {
	tests := []struct {
		Expressions []NamedExpression
		Source      Node
		Keep        bool

		want *metadata.NodeMetadata
	}{
		{
			Expressions: []NamedExpression{
				NewVariable(octosql.NewVariableName("test")),
				NewVariable(octosql.NewVariableName("test2")),
			},
			Source: NewStubNode(metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName(""),
				metadata.EmptyNamespace())),

			Keep: false,

			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName(""),
				metadata.EmptyNamespace(),
			),
		},
		{
			Expressions: []NamedExpression{
				NewVariable(octosql.NewVariableName("test")),
				NewVariable(octosql.NewVariableName("test2")),
			},
			Source: NewStubNode(metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName("my_time_field"),
				metadata.EmptyNamespace())),

			Keep: false,

			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName(""),
				metadata.EmptyNamespace(),
			),
		},
		{
			Expressions: []NamedExpression{
				NewVariable(octosql.NewVariableName("test")),
				NewVariable(octosql.NewVariableName("test2")),
			},
			Source: NewStubNode(metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName("my_time_field"),
				metadata.EmptyNamespace())),

			Keep: true,

			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName("my_time_field"),
				metadata.EmptyNamespace(),
			),
		},
		{
			Expressions: []NamedExpression{
				NewVariable(octosql.NewVariableName("test")),
				NewVariable(octosql.NewVariableName("test2")),
				NewVariable(octosql.NewVariableName("my_time_field")),
				NewVariable(octosql.NewVariableName("test3")),
			},
			Source: NewStubNode(metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName("my_time_field"),
				metadata.EmptyNamespace())),

			Keep: false,

			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName("my_time_field"),
				metadata.EmptyNamespace(),
			),
		},
		{
			Expressions: []NamedExpression{
				NewVariable(octosql.NewVariableName("test")),
				NewVariable(octosql.NewVariableName("test2")),
				NewAliasedExpression(
					octosql.NewVariableName("my_time_field_1"),
					NewVariable(octosql.NewVariableName("my_time_field")),
				),
				NewVariable(octosql.NewVariableName("test3")),
			},
			Source: NewStubNode(metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName("my_time_field"),
				metadata.EmptyNamespace())),

			Keep: false,

			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName("my_time_field_1"),
				metadata.EmptyNamespace(),
			),
		},
		{
			Expressions: []NamedExpression{
				NewVariable(octosql.NewVariableName("test")),
				NewVariable(octosql.NewVariableName("test2")),
				NewAliasedExpression(
					octosql.NewVariableName("my_time_field_4"),
					NewAliasedExpression(
						octosql.NewVariableName("my_time_field_3"),
						NewAliasedExpression(
							octosql.NewVariableName("my_time_field_2"),
							NewAliasedExpression(
								octosql.NewVariableName("my_time_field_1"),
								NewVariable(octosql.NewVariableName("my_time_field")),
							),
						),
					),
				),
				NewVariable(octosql.NewVariableName("test3")),
			},
			Source: NewStubNode(metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName("my_time_field"),
				metadata.EmptyNamespace())),

			Keep: false,

			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName("my_time_field_4"),
				metadata.EmptyNamespace(),
			),
		},
		{
			Expressions: []NamedExpression{
				NewVariable(octosql.NewVariableName("test")),
				NewVariable(octosql.NewVariableName("test2")),
				NewAliasedExpression(
					octosql.NewVariableName("my_time_field_4"),
					NewAliasedExpression(
						octosql.NewVariableName("my_time_field_3"),
						NewAliasedExpression(
							octosql.NewVariableName("my_time_field_2"),
							NewAliasedExpression(
								octosql.NewVariableName("my_time_field_1"),
								NewVariable(octosql.NewVariableName("my_time_field")),
							),
						),
					),
				),
				NewVariable(octosql.NewVariableName("test3")),
			},
			Source: NewStubNode(metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName("my_time_field"),
				metadata.EmptyNamespace())),

			Keep: true,

			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName("my_time_field"),
				metadata.EmptyNamespace(),
			),
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			node := &Map{
				Expressions: tt.Expressions,
				Source:      tt.Source,
				Keep:        tt.Keep,
			}

			got := node.Metadata()

			areNamespacesEqual := got.Namespace().Equal(tt.want.Namespace())

			if got.EventTimeField() != tt.want.EventTimeField() || got.Cardinality() != tt.want.Cardinality() || !areNamespacesEqual {
				t.Errorf("Metadata() = %v, want %v", got, tt.want)
			}
		})
	}
}
