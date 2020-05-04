package mysql

import (
	"reflect"
	"testing"

	"github.com/cube2222/octosql/datasources/sql"
	"github.com/cube2222/octosql/physical"
)

func TestFormulaToSQL(t *testing.T) {
	type args struct {
		formula physical.Formula
		aliases *mySQLPlaceholders
	}
	tests := []struct {
		name        string
		args        args
		want        string
		wantAliases *mySQLPlaceholders
	}{
		{
			name: "simple formula test",
			args: args{
				formula: physical.NewPredicate(
					physical.NewVariable("u.id"),
					physical.MoreThan,
					physical.NewVariable("const_0"),
				),
				aliases: newMySQLPlaceholders("u"),
			},
			want: "(u.id) > (?)",
			wantAliases: &mySQLPlaceholders{
				PlaceholderToExpression: []physical.Expression{
					physical.NewVariable("const_0"),
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
						physical.NewVariable("const_0"),
						physical.Equal,
						physical.NewVariable("table.age"),
					),
				),
				aliases: newMySQLPlaceholders("table"),
			},
			want: "(TRUE) AND ((?) = (table.age))",
			wantAliases: &mySQLPlaceholders{
				PlaceholderToExpression: []physical.Expression{
					physical.NewVariable("const_0"),
				},
				Alias: "table",
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
				aliases: newMySQLPlaceholders("alias"),
			},
			want: "((alias.age) <> (alias.IQ)) OR (NOT (FALSE))",
			wantAliases: &mySQLPlaceholders{
				PlaceholderToExpression: []physical.Expression{},
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
				aliases: newMySQLPlaceholders("a"),
			},
			want: "(?) > (?)",
			wantAliases: &mySQLPlaceholders{
				PlaceholderToExpression: []physical.Expression{
					physical.NewVariable("b.age"),
					physical.NewVariable("c.age"),
				},
				Alias: "a",
			},
		},
		{
			name: "complicated test with different formulas and mySQLPlaceholders",
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

				aliases: newMySQLPlaceholders("a"),
			},

			want: "(((a.age) > (?)) AND ((a.sex) = (?))) OR ((?) < (?))",
			wantAliases: &mySQLPlaceholders{
				PlaceholderToExpression: []physical.Expression{
					physical.NewVariable("b.age"),
					physical.NewVariable("const_0"),
					physical.NewVariable("const_1"),
					physical.NewVariable("b.id"),
				},
				Alias: "a",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sql.FormulaToSQL(tt.args.formula, tt.args.aliases); got != tt.want {
				t.Errorf("formulaToSQL() = %v, want %v", got, tt.want)
			}

			if !reflect.DeepEqual(tt.args.aliases, tt.wantAliases) {
				t.Errorf("formulaToSQL mySQLPlaceholders = %v, want %v", tt.args.aliases, tt.wantAliases)
			}
		})
	}
}
