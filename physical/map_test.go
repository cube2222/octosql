package physical

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/physical/metadata"
)

type StubNode struct {
	metadata *metadata.NodeMetadata
}

func (s *StubNode) Transform(ctx context.Context, transformers *Transformers) Node {
	panic("unreachable")
}

func (s *StubNode) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	panic("unreachable")
}

func (s *StubNode) Metadata() *metadata.NodeMetadata {
	return s.metadata
}

func (s *StubNode) Visualize() *graph.Node {
	panic("unreachable")
}

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
			Source: &StubNode{
				metadata: metadata.NewNodeMetadata(
					metadata.Unbounded,
					octosql.NewVariableName(""),
				),
			},
			Keep: false,

			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName(""),
			),
		},
		{
			Expressions: []NamedExpression{
				NewVariable(octosql.NewVariableName("test")),
				NewVariable(octosql.NewVariableName("test2")),
			},
			Source: &StubNode{
				metadata: metadata.NewNodeMetadata(
					metadata.Unbounded,
					octosql.NewVariableName("my_time_field"),
				),
			},
			Keep: false,

			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName(""),
			),
		},
		{
			Expressions: []NamedExpression{
				NewVariable(octosql.NewVariableName("test")),
				NewVariable(octosql.NewVariableName("test2")),
			},
			Source: &StubNode{
				metadata: metadata.NewNodeMetadata(
					metadata.Unbounded,
					octosql.NewVariableName("my_time_field"),
				),
			},
			Keep: true,

			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName("my_time_field"),
			),
		},
		{
			Expressions: []NamedExpression{
				NewVariable(octosql.NewVariableName("test")),
				NewVariable(octosql.NewVariableName("test2")),
				NewVariable(octosql.NewVariableName("my_time_field")),
				NewVariable(octosql.NewVariableName("test3")),
			},
			Source: &StubNode{
				metadata: metadata.NewNodeMetadata(
					metadata.Unbounded,
					octosql.NewVariableName("my_time_field"),
				),
			},
			Keep: false,

			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName("my_time_field"),
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
			Source: &StubNode{
				metadata: metadata.NewNodeMetadata(
					metadata.Unbounded,
					octosql.NewVariableName("my_time_field"),
				),
			},
			Keep: false,

			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName("my_time_field_1"),
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
			Source: &StubNode{
				metadata: metadata.NewNodeMetadata(
					metadata.Unbounded,
					octosql.NewVariableName("my_time_field"),
				),
			},
			Keep: false,

			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName("my_time_field_4"),
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
			Source: &StubNode{
				metadata: metadata.NewNodeMetadata(
					metadata.Unbounded,
					octosql.NewVariableName("my_time_field"),
				),
			},
			Keep: true,

			want: metadata.NewNodeMetadata(
				metadata.Unbounded,
				octosql.NewVariableName("my_time_field"),
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
			if got := node.Metadata(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Metadata() = %v, want %v", got, tt.want)
			}
		})
	}
}
