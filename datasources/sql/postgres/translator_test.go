package postgres

import (
	"reflect"
	"testing"

	"github.com/cube2222/octosql/datasources/sql"
	"github.com/cube2222/octosql/physical"
)

func TestFormulaToSQL(t *testing.T) {
	type args struct {
		formula physical.Formula
		aliases *postgresPlaceholders
	}
	tests := []struct {
		name        string
		args        args
		want        string
		wantAliases *postgresPlaceholders
	}{
		{
			name: "simple formula test",
			args: args{
				formula: physical.NewPredicate(
					physical.NewVariable("u.id"),
					physical.MoreThan,
					physical.NewVariable("const_0"),
				),
				aliases: newPostgresPlaceholders("u"),
			},
			want: "(u.id) > ($1)",
			wantAliases: &postgresPlaceholders{
				PlaceholderToExpression: map[string]physical.Expression{
					"$1": physical.NewVariable("const_0"),
				},
				Alias:   "u",
				Counter: 2,
			},
		},
		{
			name: "simple test for AND with longer alias",
			args: args{
				formula: physical.NewAnd(
					physical.NewConstant(true),
					physical.NewPredicate(
						physical.NewVariable("const_0"),
						physical.Equal,
						physical.NewVariable("table.age"),
					),
				),
				aliases: newPostgresPlaceholders("table"),
			},
			want: "(TRUE) AND (($1) = (table.age))",
			wantAliases: &postgresPlaceholders{
				PlaceholderToExpression: map[string]physical.Expression{
					"$1": physical.NewVariable("const_0"),
				},
				Alias:   "table",
				Counter: 2,
			},
		},
		{
			name: "test for OR and NOT",
			args: args{
				formula: physical.NewOr(
					physical.NewPredicate(
						physical.NewVariable("alias.age"),
						physical.NotEqual,
						physical.NewVariable("alias.IQ"),
					),
					physical.NewNot(
						physical.NewConstant(false),
					),
				),
				aliases: newPostgresPlaceholders("alias"),
			},
			want: "((alias.age) <> (alias.IQ)) OR (NOT (FALSE))",
			wantAliases: &postgresPlaceholders{
				PlaceholderToExpression: map[string]physical.Expression{},
				Counter:                 1,
				Alias:                   "alias",
			},
		},
		{
			name: "simple test with more than one alias",
			args: args{
				formula: physical.NewPredicate(
					physical.NewVariable("b.age"),
					physical.MoreThan,
					physical.NewVariable("c.age"),
				),
				aliases: newPostgresPlaceholders("a"),
			},
			want: "($1) > ($2)",
			wantAliases: &postgresPlaceholders{
				PlaceholderToExpression: map[string]physical.Expression{
					"$1": physical.NewVariable("b.age"),
					"$2": physical.NewVariable("c.age"),
				},
				Counter: 3,
				Alias:   "a",
			},
		},
		{
			name: "complicated test with different formulas and postgresPlaceholders",
			// ((a.age > b.age AND a.sex = const_0) OR (const_1 < b.id)
			args: args{
				formula: physical.NewOr(

					physical.NewAnd(
						physical.NewPredicate( // a.age > b.age
							physical.NewVariable("a.age"),
							physical.MoreThan,
							physical.NewVariable("b.age"),
						),

						physical.NewPredicate( // a.age = const_0
							physical.NewVariable("a.sex"),
							physical.Equal,
							physical.NewVariable("const_0"),
						),
					),

					physical.NewPredicate( // const_1 < b.id
						physical.NewVariable("const_1"),
						physical.LessThan,
						physical.NewVariable("b.id"),
					),
				),

				aliases: newPostgresPlaceholders("a"),
			},

			want: "(((a.age) > ($1)) AND ((a.sex) = ($2))) OR (($3) < ($4))",
			wantAliases: &postgresPlaceholders{
				PlaceholderToExpression: map[string]physical.Expression{
					"$1": physical.NewVariable("b.age"),
					"$2": physical.NewVariable("const_0"),
					"$3": physical.NewVariable("const_1"),
					"$4": physical.NewVariable("b.id"),
				},
				Counter: 5,
				Alias:   "a",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sql.FormulaToSQL(tt.args.formula, tt.args.aliases); got != tt.want {
				t.Errorf("formulaToSQL() = %v, want %v", got, tt.want)
			}

			if !reflect.DeepEqual(tt.args.aliases, tt.wantAliases) {
				t.Errorf("formulaToSQL postgresPlaceholders = %v, want %v", tt.args.aliases, tt.wantAliases)
			}
		})
	}
}
