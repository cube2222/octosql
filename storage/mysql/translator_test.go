package mysql

import (
	"reflect"
	"testing"

	"github.com/cube2222/octosql/physical"
)

func TestFormulaToSQL(t *testing.T) {
	type args struct {
		formula physical.Formula
		aliases *aliases
	}
	tests := []struct {
		name        string
		args        args
		want        string
		wantAliases *aliases
	}{
		{
			name: "simple formula test",
			args: args{
				formula: physical.NewPredicate(
					physical.NewVariable(octosql.NewVariableName("u.id")),
					physical.MoreThan,
					physical.NewVariable(octosql.NewVariableName("const_0")),
				),
				aliases: newAliases("u"),
			},
			want: "(u.id) > (?)",
			wantAliases: &aliases{
				PlaceholderToExpression: []physical.Expression{
					physical.NewVariable(octosql.NewVariableName("const_0")),
				},
				Alias: "u",
			},
		},
		{
			name: "simple test for AND with longer alias",
			args: args{
				formula: physical.NewAnd(
					physical.NewConstant(true),
					physical.NewPredicate(
						physical.NewVariable(octosql.NewVariableName("const_0")),
						physical.Equal,
						physical.NewVariable(octosql.NewVariableName("table.age")),
					),
				),
				aliases: newAliases("table"),
			},
			want: "(TRUE) AND ((?) = (table.age))",
			wantAliases: &aliases{
				PlaceholderToExpression: []physical.Expression{
					physical.NewVariable(octosql.NewVariableName("const_0")),
				},
				Alias: "table",
			},
		},
		{
			name: "test for OR and NOT",
			args: args{
				formula: physical.NewOr(
					physical.NewPredicate(
						physical.NewVariable(octosql.NewVariableName("alias.age")),
						physical.NotEqual,
						physical.NewVariable(octosql.NewVariableName("alias.IQ")),
					),
					physical.NewNot(
						physical.NewConstant(false),
					),
				),
				aliases: newAliases("alias"),
			},
			want: "((alias.age) <> (alias.IQ)) OR (NOT (FALSE))",
			wantAliases: &aliases{
				PlaceholderToExpression: []physical.Expression{},
				Alias:                   "alias",
			},
		},
		{
			name: "simple test with more than one alias",
			args: args{
				formula: physical.NewPredicate(
					physical.NewVariable(octosql.NewVariableName("b.age")),
					physical.MoreThan,
					physical.NewVariable(octosql.NewVariableName("c.age")),
				),
				aliases: newAliases("a"),
			},
			want: "(?) > (?)",
			wantAliases: &aliases{
				PlaceholderToExpression: []physical.Expression{
					physical.NewVariable(octosql.NewVariableName("b.age")),
					physical.NewVariable(octosql.NewVariableName("c.age")),
				},
				Alias: "a",
			},
		},
		{
			name: "complicated test with different formulas and aliases",
			//((a.age > b.age AND a.sex = const_0) OR (const_1 < b.id)
			args: args{
				formula: physical.NewOr(

					physical.NewAnd(
						physical.NewPredicate( //a.age > b.age
							physical.NewVariable(octosql.NewVariableName("a.age")),
							physical.MoreThan,
							physical.NewVariable(octosql.NewVariableName("b.age")),
						),

						physical.NewPredicate( //a.age = const_0
							physical.NewVariable(octosql.NewVariableName("a.sex")),
							physical.Equal,
							physical.NewVariable(octosql.NewVariableName("const_0")),
						),
					),

					physical.NewPredicate( //const_1 < b.id
						physical.NewVariable(octosql.NewVariableName("const_1")),
						physical.LessThan,
						physical.NewVariable(octosql.NewVariableName("b.id")),
					),
				),

				aliases: newAliases("a"),
			},

			want: "(((a.age) > (?)) AND ((a.sex) = (?))) OR ((?) < (?))",
			wantAliases: &aliases{
				PlaceholderToExpression: []physical.Expression{
					physical.NewVariable(octosql.NewVariableName("b.age")),
					physical.NewVariable(octosql.NewVariableName("const_0")),
					physical.NewVariable(octosql.NewVariableName("const_1")),
					physical.NewVariable(octosql.NewVariableName("b.id")),
				},
				Alias: "a",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formulaToSQL(tt.args.formula, tt.args.aliases); got != tt.want {
				t.Errorf("formulaToSQL() = %v, want %v", got, tt.want)
			}

			if !reflect.DeepEqual(tt.args.aliases, tt.wantAliases) {
				t.Errorf("formulaToSQL aliases = %v, want %v", tt.args.aliases, tt.wantAliases)
			}
		})
	}
}
