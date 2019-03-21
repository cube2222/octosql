package parser

import (
	"github.com/bradleyjkemp/memviz"
	"log"
	"os"
	"testing"

	"github.com/cube2222/octosql/logical"
	"github.com/xwb1989/sqlparser"
)

func TestParseNode(t *testing.T) {
	type args struct {
		statement string
	}
	tests := []struct {
		name    string
		args    args
		want    logical.Node
		wantErr bool
	}{
		{
			name: "simple union all",
			args: args{
				"SELECT p2.name, p2.age FROM people p2 WHERE p2.age > 3 " +
					"UNION ALL " +
					"SELECT p2.name, p2.age FROM people p2 WHERE p2.age > 4",
			},
			want: logical.NewUnionAll(
				logical.NewMap(
					[]logical.NamedExpression{
						logical.NewVariable("p2.name"),
						logical.NewVariable("p2.age"),
					},
					logical.NewFilter(
						logical.NewPredicate(
							logical.NewVariable("p2.age"),
							logical.MoreThan,
							logical.NewConstant(3),
						),
						logical.NewDataSource("people", "p2"),
					),
				),
				logical.NewMap(
					[]logical.NamedExpression{
						logical.NewVariable("p2.name"),
						logical.NewVariable("p2.age"),
					},
					logical.NewFilter(
						logical.NewPredicate(
							logical.NewVariable("p2.age"),
							logical.MoreThan,
							logical.NewConstant(4),
						),
						logical.NewDataSource("people", "p2"),
					),
				),
			),
			wantErr: false,
		},
		{
			name: "complex union all",
			args: args{
				"(SELECT p2.name, p2.age FROM people p2 WHERE p2.age > 3 UNION ALL SELECT p2.name, p2.age FROM people p2 WHERE p2.age < 5) " +
					"UNION ALL" +
					" (SELECT p2.name, p2.age FROM people p2 WHERE p2.city > 'ciechanowo' UNION ALL SELECT p2.name, p2.age FROM people p2 WHERE p2.city < 'wwa')",
			},
			want: logical.NewUnionAll(
				logical.NewUnionAll(
					logical.NewMap(
						[]logical.NamedExpression{
							logical.NewVariable("p2.name"),
							logical.NewVariable("p2.age"),
						},
						logical.NewFilter(
							logical.NewPredicate(
								logical.NewVariable("p2.age"),
								logical.MoreThan,
								logical.NewConstant(3),
							),
							logical.NewDataSource("people", "p2"),
						),
					),
					logical.NewMap(
						[]logical.NamedExpression{
							logical.NewVariable("p2.name"),
							logical.NewVariable("p2.age"),
						},
						logical.NewFilter(
							logical.NewPredicate(
								logical.NewVariable("p2.age"),
								logical.LessThan,
								logical.NewConstant(5),
							),
							logical.NewDataSource("people", "p2"),
						),
					),
				),
				logical.NewUnionAll(
					logical.NewMap(
						[]logical.NamedExpression{
							logical.NewVariable("p2.name"),
							logical.NewVariable("p2.age"),
						},
						logical.NewFilter(
							logical.NewPredicate(
								logical.NewVariable("p2.city"),
								logical.MoreThan,
								logical.NewConstant("ciechanowo"),
							),
							logical.NewDataSource("people", "p2"),
						),
					),
					logical.NewMap(
						[]logical.NamedExpression{
							logical.NewVariable("p2.name"),
							logical.NewVariable("p2.age"),
						},
						logical.NewFilter(
							logical.NewPredicate(
								logical.NewVariable("p2.city"),
								logical.LessThan,
								logical.NewConstant("wwa"),
							),
							logical.NewDataSource("people", "p2"),
						),
					),
				),
			),
			wantErr: false,
		},
		{
			name: "simple select",
			args: args{
				statement: "SELECT p2.name, p2.age FROM people p2 WHERE p2.age > 3",
			},
			want: logical.NewMap(
				[]logical.NamedExpression{
					logical.NewVariable("p2.name"),
					logical.NewVariable("p2.age"),
				},
				logical.NewFilter(
					logical.NewPredicate(
						logical.NewVariable("p2.age"),
						logical.MoreThan,
						logical.NewConstant(3),
					),
					logical.NewDataSource("people", "p2"),
				),
			),
			wantErr: false,
		},
		{
			name: "all operators",
			args: args{
				statement: "SELECT * FROM people p2 WHERE TRUE AND FALSE OR TRUE AND NOT TRUE",
			},
			want: logical.NewFilter(
				logical.NewInfixOperator(
					logical.NewInfixOperator(
						logical.NewBooleanConstant(true),
						logical.NewBooleanConstant(false),
						"AND",
					),
					logical.NewInfixOperator(
						logical.NewBooleanConstant(true),
						logical.NewPrefixOperator(logical.NewBooleanConstant(true), "NOT"),
						"AND",
					),
					"OR",
				),
				logical.NewDataSource("people", "p2"),
			),
			wantErr: false,
		},
		{
			name: "all relations",
			args: args{
				statement: `
SELECT * 
FROM people p2 
WHERE p2.age > 3 AND p2.age = 3 AND p2.age < 3 AND p2.age <> 3 AND p2.age != 3 AND p2.age IN (SELECT * FROM people p3)`,
			},
			want: logical.NewFilter(
				logical.NewInfixOperator(
					logical.NewInfixOperator(
						logical.NewInfixOperator(
							logical.NewInfixOperator(
								logical.NewInfixOperator(
									logical.NewPredicate(
										logical.NewVariable("p2.age"),
										logical.MoreThan,
										logical.NewConstant(3),
									),
									logical.NewPredicate(
										logical.NewVariable("p2.age"),
										logical.Equal,
										logical.NewConstant(3),
									),
									"AND",
								),
								logical.NewPredicate(
									logical.NewVariable("p2.age"),
									logical.LessThan,
									logical.NewConstant(3),
								),
								"AND",
							),
							logical.NewPredicate(
								logical.NewVariable("p2.age"),
								logical.NotEqual,
								logical.NewConstant(3),
							),
							"AND",
						),
						logical.NewPredicate(
							logical.NewVariable("p2.age"),
							logical.NotEqual,
							logical.NewConstant(3),
						),
						"AND",
					),
					logical.NewPredicate(
						logical.NewVariable("p2.age"),
						logical.In,
						logical.NewNodeExpression(logical.NewDataSource("people", "p3")),
					),
					"AND",
				),
				logical.NewDataSource("people", "p2"),
			),
			wantErr: false,
		},
		{
			name: "complicated select",
			args: args{
				statement: `
SELECT p3.name, (SELECT p1.city FROM people p1 WHERE p3.name = 'Kuba' AND p1.name = 'adam') as city
FROM (Select * from people p4) p3
WHERE (SELECT p2.age FROM people p2 WHERE p2.name = 'wojtek') > p3.age`,
			},
			want: logical.NewMap(
				[]logical.NamedExpression{
					logical.NewVariable("p3.name"),
					logical.NewAliasedExpression(
						"city",
						logical.NewNodeExpression(
							logical.NewMap(
								[]logical.NamedExpression{
									logical.NewVariable("p1.city"),
								},
								logical.NewFilter(
									logical.NewInfixOperator(
										logical.NewPredicate(
											logical.NewVariable("p3.name"),
											logical.Equal,
											logical.NewConstant("Kuba"),
										),
										logical.NewPredicate(
											logical.NewVariable("p1.name"),
											logical.Equal,
											logical.NewConstant("adam"),
										),
										"AND",
									),
									logical.NewDataSource("people", "p1"),
								),
							),
						),
					),
				},
				logical.NewFilter(
					logical.NewPredicate(
						logical.NewNodeExpression(
							logical.NewMap(
								[]logical.NamedExpression{
									logical.NewVariable("p2.age"),
								},
								logical.NewFilter(
									logical.NewPredicate(
										logical.NewVariable("p2.name"),
										logical.Equal,
										logical.NewConstant("wojtek"),
									),
									logical.NewDataSource("people", "p2"),
								),
							),
						),
						logical.MoreThan,
						logical.NewVariable("p3.age"),
					),
					logical.NewRequalifier(
						"p3",
						logical.NewDataSource("people", "p4"),
					),
				),
			),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.Parse(tt.args.statement)
			if err != nil {
				t.Fatal(err)
			}

			statement := stmt.(sqlparser.SelectStatement)

			got, err := ParseNode(statement)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseNode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err := logical.EqualNodes(got, tt.want); err != nil {
				f, err := os.Create("diag_got")
				if err != nil {
					log.Fatal(err)
				}
				memmap.Map(f, got)
				f.Close()

				f, err = os.Create("diag_wanted")
				if err != nil {
					log.Fatal(err)
				}
				memmap.Map(f, tt.want)
				f.Close()
				t.Errorf("ParseNode() = %v, want %v: %v", got, tt.want, err)
			}
		})
	}
}
