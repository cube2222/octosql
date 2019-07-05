package parser

import (
	"log"
	"os"
	"testing"

	"github.com/bradleyjkemp/memviz"

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
			name: "simple union",
			args: args{
				`SELECT p.id, p.name, p.surname FROM people p WHERE p.surname = 'Kowalski'
					UNION
					SELECT * FROM admins a WHERE a.exp < 2`,
			},
			want: logical.NewUnionDistinct(
				logical.NewMap(
					[]logical.NamedExpression{
						logical.NewVariable("p.id"),
						logical.NewVariable("p.name"),
						logical.NewVariable("p.surname"),
					},
					logical.NewFilter(
						logical.NewPredicate(
							logical.NewVariable("p.surname"),
							logical.Equal,
							logical.NewConstant("Kowalski"),
						),
						logical.NewMap(
							[]logical.NamedExpression{
								logical.NewVariable("p.id"),
								logical.NewVariable("p.name"),
								logical.NewVariable("p.surname"),
							},
							logical.NewDataSource("people", "p"),
							false),
					),
					false,
				),
				logical.NewFilter(
					logical.NewPredicate(
						logical.NewVariable("a.exp"),
						logical.LessThan,
						logical.NewConstant(2),
					),
					logical.NewDataSource(
						"admins",
						"a",
					),
				),
			),
			wantErr: false,
		},
		{
			name: "simple union all + limit + NO offset",
			args: args{
				`SELECT c.name, c.age FROM cities c WHERE c.age > 100
					UNION ALL
					SELECT p.name, p.age FROM people p WHERE p.age > 4
					LIMIT 5`,
			},
			want: logical.NewLimit(
				logical.NewUnionAll(
					logical.NewMap(
						[]logical.NamedExpression{
							logical.NewVariable("c.name"),
							logical.NewVariable("c.age"),
						},
						logical.NewFilter(
							logical.NewPredicate(
								logical.NewVariable("c.age"),
								logical.MoreThan,
								logical.NewConstant(100),
							),
							logical.NewMap(
								[]logical.NamedExpression{
									logical.NewVariable("c.name"),
									logical.NewVariable("c.age"),
								},
								logical.NewDataSource("cities", "c"),
								true,
							),
						),
						false,
					),
					logical.NewMap(
						[]logical.NamedExpression{
							logical.NewVariable("p.name"),
							logical.NewVariable("p.age"),
						},
						logical.NewFilter(
							logical.NewPredicate(
								logical.NewVariable("p.age"),
								logical.MoreThan,
								logical.NewConstant(4),
							),
							logical.NewMap(
								[]logical.NamedExpression{
									logical.NewVariable("p.name"),
									logical.NewVariable("p.age"),
								},
								logical.NewDataSource("people", "p"),
								true,
							),
						),
						false,
					),
				),
				logical.NewConstant(5),
			),
			wantErr: false,
		},
		{
			name: "simple limit + offset",
			args: args{
				"SELECT p.name, p.age FROM people p LIMIT 3 OFFSET 2",
			},
			want: logical.NewLimit(
				logical.NewOffset(
					logical.NewMap(
						[]logical.NamedExpression{
							logical.NewVariable("p.name"),
							logical.NewVariable("p.age"),
						},
						logical.NewMap(
							[]logical.NamedExpression{
								logical.NewVariable("p.name"),
								logical.NewVariable("p.age"),
							},
							logical.NewDataSource("people", "p"),
							true,
						),
						false,
					),
					logical.NewConstant(2),
				),
				logical.NewConstant(3),
			),
			wantErr: false,
		},
		{
			name: "simple union all",
			args: args{
				`SELECT p2.name, p2.age FROM people p2 WHERE p2.age > 3
					UNION ALL
					SELECT p2.name, p2.age FROM people p2 WHERE p2.age > 4`,
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
						logical.NewMap(
							[]logical.NamedExpression{
								logical.NewVariable("p2.name"),
								logical.NewVariable("p2.age"),
							},
							logical.NewDataSource("people", "p2"),
							true,
						),
					),
					false,
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
						logical.NewMap(
							[]logical.NamedExpression{
								logical.NewVariable("p2.name"),
								logical.NewVariable("p2.age"),
							},
							logical.NewDataSource("people", "p2"),
							true,
						),
					),
					false,
				),
			),
			wantErr: false,
		},
		{
			name: "complex union all",
			args: args{
				`(SELECT p2.name, p2.age FROM people p2 WHERE p2.age > 3 UNION ALL SELECT p2.name, p2.age FROM people p2 WHERE p2.age < 5)
					UNION ALL
					(SELECT p2.name, p2.age FROM people p2 WHERE p2.city > 'ciechanowo' UNION ALL SELECT p2.name, p2.age FROM people p2 WHERE p2.city < 'wwa')`,
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
							logical.NewMap(
								[]logical.NamedExpression{
									logical.NewVariable("p2.name"),
									logical.NewVariable("p2.age"),
								},
								logical.NewDataSource("people", "p2"),
								true,
							),
						),
						false,
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
							logical.NewMap(
								[]logical.NamedExpression{
									logical.NewVariable("p2.name"),
									logical.NewVariable("p2.age"),
								},
								logical.NewDataSource("people", "p2"),
								true,
							),
						),
						false,
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
							logical.NewMap(
								[]logical.NamedExpression{
									logical.NewVariable("p2.name"),
									logical.NewVariable("p2.age"),
								},
								logical.NewDataSource("people", "p2"),
								true,
							),
						),
						false,
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
							logical.NewMap(
								[]logical.NamedExpression{
									logical.NewVariable("p2.name"),
									logical.NewVariable("p2.age"),
								},
								logical.NewDataSource("people", "p2"),
								true,
							),
						),
						false,
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
					logical.NewMap(
						[]logical.NamedExpression{
							logical.NewVariable("p2.name"),
							logical.NewVariable("p2.age"),
						},
						logical.NewDataSource("people", "p2"),
						true,
					),
				),
				false,
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
					logical.NewVariable("city"),
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
									logical.NewMap(
										[]logical.NamedExpression{
											logical.NewVariable("p2.age"),
										},
										logical.NewDataSource("people", "p2"),
										true,
									),
								),
								false,
							),
						),
						logical.MoreThan,
						logical.NewVariable("p3.age"),
					),
					logical.NewMap(
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
											logical.NewMap(
												[]logical.NamedExpression{
													logical.NewVariable("p1.city"),
												},
												logical.NewDataSource("people", "p1"),
												true,
											),
										),
										false,
									),
								),
							),
						},
						logical.NewRequalifier(
							"p3",
							logical.NewDataSource("people", "p4"),
						),
						true,
					),
				),
				false,
			),
			wantErr: false,
		},
		{
			name: "left join",
			args: args{
				statement: `
SELECT p.name FROM people p LEFT JOIN cities c ON p.city = c.name AND p.favorite_city = c.name`,
			},
			want: logical.NewMap(
				[]logical.NamedExpression{
					logical.NewVariable("p.name"),
				},
				logical.NewMap(
					[]logical.NamedExpression{
						logical.NewVariable("p.name"),
					},
					logical.NewLeftJoin(
						logical.NewDataSource("people", "p"),
						logical.NewFilter(
							logical.NewInfixOperator(
								logical.NewPredicate(
									logical.NewVariable("p.city"),
									logical.Equal,
									logical.NewVariable("c.name"),
								),
								logical.NewPredicate(
									logical.NewVariable("p.favorite_city"),
									logical.Equal,
									logical.NewVariable("c.name"),
								),
								"AND",
							),
							logical.NewDataSource("cities", "c"),
						),
					),
					true,
				),
				false,
			),
			wantErr: false,
		},
		{
			name: "right join",
			args: args{
				statement: `
SELECT p.name FROM cities c RIGHT JOIN people p ON p.city = c.name AND p.favorite_city = c.name`,
			},
			want: logical.NewMap(
				[]logical.NamedExpression{
					logical.NewVariable("p.name"),
				},
				logical.NewMap(
					[]logical.NamedExpression{
						logical.NewVariable("p.name"),
					},
					logical.NewLeftJoin(
						logical.NewDataSource("people", "p"),
						logical.NewFilter(
							logical.NewInfixOperator(
								logical.NewPredicate(
									logical.NewVariable("p.city"),
									logical.Equal,
									logical.NewVariable("c.name"),
								),
								logical.NewPredicate(
									logical.NewVariable("p.favorite_city"),
									logical.Equal,
									logical.NewVariable("c.name"),
								),
								"AND",
							),
							logical.NewDataSource("cities", "c"),
						),
					),
					true,
				),
				false,
			),
			wantErr: false,
		},
		{
			name: "right join",
			args: args{
				statement: `SELECT * FROM people p WHERE p.age IN (1, 2, 3, 4) AND ('Jacob', 3) IN (SELECT * FROM people p)`,
			},
			want: logical.NewFilter(
				logical.NewInfixOperator(
					logical.NewPredicate(
						logical.NewVariable("p.age"),
						logical.In,
						logical.NewTuple([]logical.Expression{
							logical.NewConstant(1),
							logical.NewConstant(2),
							logical.NewConstant(3),
							logical.NewConstant(4),
						}),
					),
					logical.NewPredicate(
						logical.NewTuple([]logical.Expression{
							logical.NewConstant("Jacob"),
							logical.NewConstant(3),
						}),
						logical.In,
						logical.NewNodeExpression(
							logical.NewDataSource("people", "p"),
						),
					),
					"AND"),
				logical.NewDataSource("people", "p"),
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
				t.Errorf("ParseNode() = %v, want %v: %v", got, tt.want, err)

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
			}
		})
	}
}
