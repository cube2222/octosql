package postgres

import (
	"testing"

	"github.com/cube2222/octosql/physical"
)

func TestFormulaToSQL(t *testing.T) {
	type args struct {
		formula physical.Formula
		aliases *Aliases
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "simple formula test",
			args: args{
				formula: physical.NewPredicate(
					physical.NewVariable("u.id"),
					physical.MoreThan,
					physical.NewVariable("const_0"),
				),
				aliases: NewAliases("u"),
			},
			want: "(u.id) > ($1)",
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
				aliases: NewAliases("table"),
			},
			want: "(TRUE) AND (($1) = (table.age))",
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
				aliases: NewAliases("alias"),
			},
			want: "((alias.age) <> (alias.IQ)) OR (NOT (FALSE))",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FormulaToSQL(tt.args.formula, tt.args.aliases); got != tt.want {
				t.Errorf("FormulaToSQL() = %v, want %v", got, tt.want)
			}
		})
	}
}
