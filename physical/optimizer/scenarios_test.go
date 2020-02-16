package optimizer

import (
	"context"
	"reflect"
	"testing"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/physical"
)

func TestMergeRequalifiers(t *testing.T) {
	type args struct {
		plan physical.Node
	}
	tests := []struct {
		name string
		args args
		want physical.Node
	}{
		{
			name: "simple single merge",
			args: args{
				plan: &physical.Requalifier{
					Qualifier: "a",
					Source: &physical.Requalifier{
						Qualifier: "b",
						Source: &PlaceholderNode{
							Name: "stub",
						},
					},
				},
			},
			want: &physical.Requalifier{
				Qualifier: "a",
				Source: &PlaceholderNode{
					Name: "stub",
				},
			},
		},
		{
			name: "multi merge",
			args: args{
				plan: &physical.Requalifier{
					Qualifier: "a",
					Source: &physical.Requalifier{
						Qualifier: "b",
						Source: &physical.Requalifier{
							Qualifier: "c",
							Source: &physical.Requalifier{
								Qualifier: "d",
								Source: &physical.Requalifier{
									Qualifier: "e",
									Source: &physical.Requalifier{
										Qualifier: "f",
										Source: &physical.Requalifier{
											Qualifier: "g",
											Source: &physical.Requalifier{
												Qualifier: "h",
												Source: &PlaceholderNode{
													Name: "stub",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			want: &physical.Requalifier{
				Qualifier: "a",
				Source: &PlaceholderNode{
					Name: "stub",
				},
			},
		},
		{
			name: "deeper merge",
			args: args{
				plan: &physical.Map{
					Expressions: []physical.NamedExpression{physical.NewVariable(octosql.NewVariableName("expr"))},
					Source: &physical.Requalifier{
						Qualifier: "a",
						Source: &physical.Requalifier{
							Qualifier: "b",
							Source: &physical.Requalifier{
								Qualifier: "c",
								Source: &PlaceholderNode{
									Name: "stub",
								},
							},
						},
					},
				},
			},
			want: &physical.Map{
				Expressions: []physical.NamedExpression{physical.NewVariable(octosql.NewVariableName("expr"))},
				Source: &physical.Requalifier{
					Qualifier: "a",
					Source: &PlaceholderNode{
						Name: "stub",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Optimize(context.Background(), []Scenario{MergeRequalifiers}, tt.args.plan); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MergeRequalifiers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMergeFilters(t *testing.T) {
	type args struct {
		plan physical.Node
	}
	tests := []struct {
		name string
		args args
		want physical.Node
	}{
		{
			name: "simple single merge",
			args: args{
				plan: &physical.Filter{
					Formula: physical.NewPredicate(
						physical.NewVariable(octosql.NewVariableName("parent_left")),
						physical.Equal,
						physical.NewVariable(octosql.NewVariableName("parent_right")),
					),
					Source: &physical.Filter{
						Formula: physical.NewPredicate(
							physical.NewVariable(octosql.NewVariableName("child_left")),
							physical.NotEqual,
							physical.NewVariable(octosql.NewVariableName("child_right")),
						),
						Source: &PlaceholderNode{
							Name: "stub",
						},
					},
				},
			},
			want: &physical.Filter{
				Formula: physical.NewAnd(
					physical.NewPredicate(
						physical.NewVariable(octosql.NewVariableName("parent_left")),
						physical.Equal,
						physical.NewVariable(octosql.NewVariableName("parent_right")),
					),
					physical.NewPredicate(
						physical.NewVariable(octosql.NewVariableName("child_left")),
						physical.NotEqual,
						physical.NewVariable(octosql.NewVariableName("child_right")),
					),
				),
				Source: &PlaceholderNode{
					Name: "stub",
				},
			},
		},
		{
			name: "multi merge",
			args: args{
				plan: &physical.Filter{
					Formula: physical.NewPredicate(
						physical.NewVariable(octosql.NewVariableName("a_left")),
						physical.Equal,
						physical.NewVariable(octosql.NewVariableName("a_right")),
					),
					Source: &physical.Filter{
						Formula: physical.NewPredicate(
							physical.NewVariable(octosql.NewVariableName("b_left")),
							physical.MoreThan,
							physical.NewVariable(octosql.NewVariableName("b_right")),
						),
						Source: &physical.Filter{
							Formula: physical.NewPredicate(
								physical.NewVariable(octosql.NewVariableName("c_left")),
								physical.LessThan,
								physical.NewVariable(octosql.NewVariableName("c_right")),
							),
							Source: &physical.Filter{
								Formula: physical.NewPredicate(
									physical.NewVariable(octosql.NewVariableName("d_left")),
									physical.NotEqual,
									physical.NewVariable(octosql.NewVariableName("d_right")),
								),
								Source: &PlaceholderNode{
									Name: "stub",
								},
							},
						},
					},
				},
			},
			want: &physical.Filter{
				Formula: physical.NewAnd(
					physical.NewPredicate(
						physical.NewVariable(octosql.NewVariableName("a_left")),
						physical.Equal,
						physical.NewVariable(octosql.NewVariableName("a_right")),
					),
					physical.NewAnd(
						physical.NewPredicate(
							physical.NewVariable(octosql.NewVariableName("b_left")),
							physical.MoreThan,
							physical.NewVariable(octosql.NewVariableName("b_right")),
						),
						physical.NewAnd(
							physical.NewPredicate(
								physical.NewVariable(octosql.NewVariableName("c_left")),
								physical.LessThan,
								physical.NewVariable(octosql.NewVariableName("c_right")),
							),
							physical.NewPredicate(
								physical.NewVariable(octosql.NewVariableName("d_left")),
								physical.NotEqual,
								physical.NewVariable(octosql.NewVariableName("d_right")),
							),
						),
					),
				),
				Source: &PlaceholderNode{
					Name: "stub",
				},
			},
		},
		{
			name: "deeper merge",
			args: args{
				plan: &physical.Map{
					Expressions: []physical.NamedExpression{physical.NewVariable(octosql.NewVariableName("expr"))},
					Source: &physical.Filter{
						Formula: physical.NewPredicate(
							physical.NewVariable(octosql.NewVariableName("a_left")),
							physical.Equal,
							physical.NewVariable(octosql.NewVariableName("a_right")),
						),
						Source: &physical.Filter{
							Formula: physical.NewPredicate(
								physical.NewVariable(octosql.NewVariableName("b_left")),
								physical.MoreThan,
								physical.NewVariable(octosql.NewVariableName("b_right")),
							),
							Source: &physical.Filter{
								Formula: physical.NewPredicate(
									physical.NewVariable(octosql.NewVariableName("c_left")),
									physical.LessThan,
									physical.NewVariable(octosql.NewVariableName("c_right")),
								),
								Source: &PlaceholderNode{
									Name: "stub",
								},
							},
						},
					},
				},
			},
			want: &physical.Map{
				Expressions: []physical.NamedExpression{physical.NewVariable(octosql.NewVariableName("expr"))},
				Source: &physical.Filter{
					Formula: physical.NewAnd(
						physical.NewPredicate(
							physical.NewVariable(octosql.NewVariableName("a_left")),
							physical.Equal,
							physical.NewVariable(octosql.NewVariableName("a_right")),
						),
						physical.NewAnd(
							physical.NewPredicate(
								physical.NewVariable(octosql.NewVariableName("b_left")),
								physical.MoreThan,
								physical.NewVariable(octosql.NewVariableName("b_right")),
							),
							physical.NewPredicate(
								physical.NewVariable(octosql.NewVariableName("c_left")),
								physical.LessThan,
								physical.NewVariable(octosql.NewVariableName("c_right")),
							),
						),
					),
					Source: &PlaceholderNode{
						Name: "stub",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Optimize(context.Background(), []Scenario{MergeFilters}, tt.args.plan); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MergeFilters() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMergeDataSourceWithRequalifier(t *testing.T) {
	type args struct {
		plan physical.Node
	}
	tests := []struct {
		name string
		args args
		want physical.Node
	}{
		{
			name: "simple single merge",
			args: args{
				plan: &physical.Requalifier{
					Qualifier: "a",
					Source: &physical.DataSourceBuilder{
						Materializer: nil,
						PrimaryKeys:  []octosql.VariableName{octosql.NewVariableName("a")},
						AvailableFilters: map[physical.FieldType]map[physical.Relation]struct{}{
							physical.Primary: {
								physical.Equal:    struct{}{},
								physical.NotEqual: struct{}{},
							},
							physical.Secondary: {
								physical.Equal:    struct{}{},
								physical.NotEqual: struct{}{},
								physical.MoreThan: struct{}{},
								physical.LessThan: struct{}{},
							},
						},
						Filter: physical.NewAnd(
							physical.NewConstant(true),
							physical.NewConstant(false),
						),
						Name:  "baz",
						Alias: "b",
					},
				},
			},
			want: &physical.DataSourceBuilder{
				Materializer: nil,
				PrimaryKeys:  []octosql.VariableName{octosql.NewVariableName("a")},
				AvailableFilters: map[physical.FieldType]map[physical.Relation]struct{}{
					physical.Primary: {
						physical.Equal:    struct{}{},
						physical.NotEqual: struct{}{},
					},
					physical.Secondary: {
						physical.Equal:    struct{}{},
						physical.NotEqual: struct{}{},
						physical.MoreThan: struct{}{},
						physical.LessThan: struct{}{},
					},
				},
				Filter: physical.NewAnd(
					physical.NewConstant(true),
					physical.NewConstant(false),
				),
				Name:  "baz",
				Alias: "a",
			},
		},
		{
			name: "multi merge",
			args: args{
				plan: &physical.Requalifier{
					Qualifier: "a",
					Source: &physical.Requalifier{
						Qualifier: "b",
						Source: &physical.DataSourceBuilder{
							Materializer: nil,
							PrimaryKeys:  []octosql.VariableName{octosql.NewVariableName("a")},
							AvailableFilters: map[physical.FieldType]map[physical.Relation]struct{}{
								physical.Primary: {
									physical.Equal:    struct{}{},
									physical.NotEqual: struct{}{},
								},
								physical.Secondary: {
									physical.Equal:    struct{}{},
									physical.NotEqual: struct{}{},
									physical.MoreThan: struct{}{},
									physical.LessThan: struct{}{},
								},
							},
							Filter: physical.NewAnd(
								physical.NewConstant(true),
								physical.NewConstant(false),
							),
							Name:  "baz",
							Alias: "c",
						},
					},
				},
			},
			want: &physical.DataSourceBuilder{
				Materializer: nil,
				PrimaryKeys:  []octosql.VariableName{octosql.NewVariableName("a")},
				AvailableFilters: map[physical.FieldType]map[physical.Relation]struct{}{
					physical.Primary: {
						physical.Equal:    struct{}{},
						physical.NotEqual: struct{}{},
					},
					physical.Secondary: {
						physical.Equal:    struct{}{},
						physical.NotEqual: struct{}{},
						physical.MoreThan: struct{}{},
						physical.LessThan: struct{}{},
					},
				},
				Filter: physical.NewAnd(
					physical.NewConstant(true),
					physical.NewConstant(false),
				),
				Name:  "baz",
				Alias: "a",
			},
		},
		{
			name: "simple single merge",
			args: args{
				plan: &physical.Map{
					Expressions: []physical.NamedExpression{physical.NewVariable(octosql.NewVariableName("expr"))},
					Source: &physical.Requalifier{
						Qualifier: "a",
						Source: &physical.DataSourceBuilder{
							Materializer: nil,
							PrimaryKeys:  []octosql.VariableName{octosql.NewVariableName("a")},
							AvailableFilters: map[physical.FieldType]map[physical.Relation]struct{}{
								physical.Primary: {
									physical.Equal:    struct{}{},
									physical.NotEqual: struct{}{},
								},
								physical.Secondary: {
									physical.Equal:    struct{}{},
									physical.NotEqual: struct{}{},
									physical.MoreThan: struct{}{},
									physical.LessThan: struct{}{},
								},
							},
							Filter: physical.NewAnd(
								physical.NewConstant(true),
								physical.NewConstant(false),
							),
							Name:  "baz",
							Alias: "b",
						},
					},
				},
			},
			want: &physical.Map{
				Expressions: []physical.NamedExpression{physical.NewVariable(octosql.NewVariableName("expr"))},
				Source: &physical.DataSourceBuilder{
					Materializer: nil,
					PrimaryKeys:  []octosql.VariableName{octosql.NewVariableName("a")},
					AvailableFilters: map[physical.FieldType]map[physical.Relation]struct{}{
						physical.Primary: {
							physical.Equal:    struct{}{},
							physical.NotEqual: struct{}{},
						},
						physical.Secondary: {
							physical.Equal:    struct{}{},
							physical.NotEqual: struct{}{},
							physical.MoreThan: struct{}{},
							physical.LessThan: struct{}{},
						},
					},
					Filter: physical.NewAnd(
						physical.NewConstant(true),
						physical.NewConstant(false),
					),
					Name:  "baz",
					Alias: "a",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Optimize(context.Background(), []Scenario{MergeDataSourceBuilderWithRequalifier}, tt.args.plan); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MergeDataSourceWithRequalifier() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMergeDataSourceWithFilter(t *testing.T) {
	type args struct {
		plan physical.Node
	}
	tests := []struct {
		name string
		args args
		want physical.Node
	}{
		{
			name: "simple single merge",
			args: args{
				plan: &physical.Filter{
					Formula: physical.NewPredicate(
						physical.NewVariable(octosql.NewVariableName("a.name")),
						physical.Equal,
						physical.NewVariable(octosql.NewVariableName("a.surname")),
					),
					Source: &physical.DataSourceBuilder{
						Materializer: nil,
						PrimaryKeys:  []octosql.VariableName{},
						AvailableFilters: map[physical.FieldType]map[physical.Relation]struct{}{
							physical.Primary: {
								physical.Equal:    struct{}{},
								physical.NotEqual: struct{}{},
								physical.MoreThan: struct{}{},
								physical.LessThan: struct{}{},
							},
							physical.Secondary: {
								physical.Equal: struct{}{},
							},
						},
						Filter: physical.NewConstant(true),
						Name:   "baz",
						Alias:  "a",
					},
				},
			},
			want: &physical.DataSourceBuilder{
				Materializer: nil,
				PrimaryKeys:  []octosql.VariableName{},
				AvailableFilters: map[physical.FieldType]map[physical.Relation]struct{}{
					physical.Primary: {
						physical.Equal:    struct{}{},
						physical.NotEqual: struct{}{},
						physical.MoreThan: struct{}{},
						physical.LessThan: struct{}{},
					},
					physical.Secondary: {
						physical.Equal: struct{}{},
					},
				},
				Filter: physical.NewAnd(
					physical.NewPredicate(
						physical.NewVariable(octosql.NewVariableName("a.name")),
						physical.Equal,
						physical.NewVariable(octosql.NewVariableName("a.surname")),
					),
					physical.NewConstant(true),
				),
				Name:  "baz",
				Alias: "a",
			},
		},
		{
			name: "simple partial merge",
			args: args{
				plan: &physical.Filter{
					Formula: physical.NewAnd(
						physical.NewPredicate(
							physical.NewVariable(octosql.NewVariableName("a.name")),
							physical.Equal,
							physical.NewVariable(octosql.NewVariableName("a.surname")),
						),
						physical.NewPredicate(
							physical.NewVariable(octosql.NewVariableName("b.test")),
							physical.MoreThan,
							physical.NewVariable(octosql.NewVariableName("a.surname")),
						),
					),

					Source: &physical.DataSourceBuilder{
						Materializer: nil,
						PrimaryKeys:  []octosql.VariableName{octosql.NewVariableName("a.name")},
						AvailableFilters: map[physical.FieldType]map[physical.Relation]struct{}{
							physical.Primary: {
								physical.Equal:    struct{}{},
								physical.NotEqual: struct{}{},
								physical.MoreThan: struct{}{},
								physical.LessThan: struct{}{},
							},
							physical.Secondary: {
								physical.Equal: struct{}{},
							},
						},
						Filter: physical.NewConstant(true),
						Name:   "baz",
						Alias:  "a",
					},
				},
			},
			want: &physical.Filter{
				Formula: physical.NewPredicate(
					physical.NewVariable(octosql.NewVariableName("b.test")),
					physical.MoreThan,
					physical.NewVariable(octosql.NewVariableName("a.surname")),
				),
				Source: &physical.DataSourceBuilder{
					Materializer: nil,
					PrimaryKeys:  []octosql.VariableName{octosql.NewVariableName("a.name")},
					AvailableFilters: map[physical.FieldType]map[physical.Relation]struct{}{
						physical.Primary: {
							physical.Equal:    struct{}{},
							physical.NotEqual: struct{}{},
							physical.MoreThan: struct{}{},
							physical.LessThan: struct{}{},
						},
						physical.Secondary: {
							physical.Equal: struct{}{},
						},
					},
					Filter: physical.NewAnd(
						physical.NewPredicate(
							physical.NewVariable(octosql.NewVariableName("a.name")),
							physical.Equal,
							physical.NewVariable(octosql.NewVariableName("a.surname")),
						),
						physical.NewConstant(true),
					),
					Name:  "baz",
					Alias: "a",
				},
			},
		},
		{
			name: "simple partial merge",
			args: args{
				plan: &physical.Filter{
					Formula: physical.NewAnd(
						physical.NewPredicate(
							physical.NewVariable(octosql.NewVariableName("a.name")),
							physical.Equal,
							physical.NewVariable(octosql.NewVariableName("a.surname")),
						),
						physical.NewPredicate(
							physical.NewVariable(octosql.NewVariableName("b.test")),
							physical.MoreThan,
							physical.NewVariable(octosql.NewVariableName("b.test2")),
						),
					),

					Source: &physical.DataSourceBuilder{
						Materializer: nil,
						PrimaryKeys:  []octosql.VariableName{octosql.NewVariableName("a.name")},
						AvailableFilters: map[physical.FieldType]map[physical.Relation]struct{}{
							physical.Primary: {
								physical.Equal:    struct{}{},
								physical.NotEqual: struct{}{},
								physical.MoreThan: struct{}{},
								physical.LessThan: struct{}{},
							},
							physical.Secondary: {
								physical.Equal: struct{}{},
							},
						},
						Filter: physical.NewConstant(true),
						Name:   "baz",
						Alias:  "a",
					},
				},
			},
			want: &physical.Filter{
				Formula: physical.NewPredicate(
					physical.NewVariable(octosql.NewVariableName("b.test")),
					physical.MoreThan,
					physical.NewVariable(octosql.NewVariableName("b.test2")),
				),
				Source: &physical.DataSourceBuilder{
					Materializer: nil,
					PrimaryKeys:  []octosql.VariableName{octosql.NewVariableName("a.name")},
					AvailableFilters: map[physical.FieldType]map[physical.Relation]struct{}{
						physical.Primary: {
							physical.Equal:    struct{}{},
							physical.NotEqual: struct{}{},
							physical.MoreThan: struct{}{},
							physical.LessThan: struct{}{},
						},
						physical.Secondary: {
							physical.Equal: struct{}{},
						},
					},
					Filter: physical.NewAnd(
						physical.NewPredicate(
							physical.NewVariable(octosql.NewVariableName("a.name")),
							physical.Equal,
							physical.NewVariable(octosql.NewVariableName("a.surname")),
						),
						physical.NewConstant(true),
					),
					Name:  "baz",
					Alias: "a",
				},
			},
		},
		{
			name: "test",
			args: args{
				plan: &physical.Filter{
					Formula: physical.NewPredicate(
						physical.NewVariable(octosql.NewVariableName("a.name")),
						physical.Equal,
						physical.NewVariable(octosql.NewVariableName("b.surname")),
					),

					Source: &physical.DataSourceBuilder{
						Materializer: nil,
						PrimaryKeys:  []octosql.VariableName{octosql.NewVariableName("a.name")},
						AvailableFilters: map[physical.FieldType]map[physical.Relation]struct{}{
							physical.Primary: {
								physical.Equal: struct{}{},
							},
							physical.Secondary: {},
						},
						Filter: physical.NewConstant(true),
						Name:   "baz",
						Alias:  "a",
					},
				},
			},
			want: &physical.DataSourceBuilder{
				Materializer: nil,
				PrimaryKeys:  []octosql.VariableName{octosql.NewVariableName("a.name")},
				AvailableFilters: map[physical.FieldType]map[physical.Relation]struct{}{
					physical.Primary: {
						physical.Equal: struct{}{},
					},
					physical.Secondary: {},
				},
				Filter: physical.NewAnd(
					physical.NewPredicate(
						physical.NewVariable(octosql.NewVariableName("a.name")),
						physical.Equal,
						physical.NewVariable(octosql.NewVariableName("b.surname")),
					),
					physical.NewConstant(true),
				),
				Name:  "baz",
				Alias: "a",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Optimize(context.Background(), []Scenario{MergeDataSourceBuilderWithFilter}, tt.args.plan); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MergeDataSourceWithFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMultiOptimization(t *testing.T) {
	type args struct {
		plan physical.Node
	}
	tests := []struct {
		name string
		args args
		want physical.Node
	}{
		{
			name: "multiple optimizations",
			args: args{
				plan: &physical.Map{
					Expressions: []physical.NamedExpression{physical.NewVariable(octosql.NewVariableName("expr"))},
					Source: &physical.Requalifier{
						Qualifier: "a",
						Source: &physical.Requalifier{
							Qualifier: "b",
							Source: &physical.Filter{
								Formula: physical.NewConstant(true),
								Source: &physical.Filter{
									Formula: physical.NewConstant(false),
									Source: &physical.Filter{
										Formula: physical.NewConstant(true),
										Source: &physical.Requalifier{
											Qualifier: "a",
											Source: &physical.Requalifier{
												Qualifier: "b",
												Source: &physical.DataSourceBuilder{
													Materializer: nil,
													PrimaryKeys:  []octosql.VariableName{octosql.NewVariableName("a")},
													AvailableFilters: map[physical.FieldType]map[physical.Relation]struct{}{
														physical.Primary: {
															physical.Equal:    struct{}{},
															physical.NotEqual: struct{}{},
														},
														physical.Secondary: {
															physical.Equal:    struct{}{},
															physical.NotEqual: struct{}{},
															physical.MoreThan: struct{}{},
															physical.LessThan: struct{}{},
														},
													},
													Filter: physical.NewAnd(
														physical.NewConstant(true),
														physical.NewConstant(false),
													),
													Name:  "baz",
													Alias: "c",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			want: &physical.Map{
				Expressions: []physical.NamedExpression{physical.NewVariable(octosql.NewVariableName("expr"))},
				Source: &physical.Requalifier{
					Qualifier: "a",
					Source: &physical.Filter{
						Formula: physical.NewAnd(
							physical.NewConstant(true),
							physical.NewAnd(
								physical.NewConstant(false),
								physical.NewConstant(true),
							),
						),
						Source: &physical.DataSourceBuilder{
							Materializer: nil,
							PrimaryKeys:  []octosql.VariableName{octosql.NewVariableName("a")},
							AvailableFilters: map[physical.FieldType]map[physical.Relation]struct{}{
								physical.Primary: {
									physical.Equal:    struct{}{},
									physical.NotEqual: struct{}{},
								},
								physical.Secondary: {
									physical.Equal:    struct{}{},
									physical.NotEqual: struct{}{},
									physical.MoreThan: struct{}{},
									physical.LessThan: struct{}{},
								},
							},
							Filter: physical.NewAnd(
								physical.NewConstant(true),
								physical.NewConstant(false),
							),
							Name:  "baz",
							Alias: "a",
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Optimize(
				context.Background(),
				[]Scenario{
					MergeRequalifiers,
					MergeFilters,
					MergeDataSourceBuilderWithRequalifier,
				},
				tt.args.plan,
			); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MergeDataSourceWithRequalifier() = %v, want %v", got, tt.want)
			}
		})
	}
}
