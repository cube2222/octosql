package parser

import (
	"log"
	"os"
	"testing"
	"time"

	memmap "github.com/bradleyjkemp/memviz"

	"github.com/cube2222/octosql/execution"

	"github.com/cube2222/octosql"

	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/parser/sqlparser"
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
			name: "columns and qualified star expression collision",
			args: args{
				statement: `SELECT d.*, d.name FROM dogs d`,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "columns and non-qualified star expression collision",
			args: args{
				statement: `SELECT *, d.name FROM dogs d`,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "columns  collision",
			args: args{
				statement: `SELECT d.age, d.id, d.id2, d.age, d.name FROM dogs d`,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "qualified stars collision",
			args: args{
				statement: `SELECT a.*, d.a, d.b, a.*, d.c FROM dogs d`,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "simple select - no stars, no filter",
			args: args{
				statement: `SELECT a.age, a.name FROM anacondas a`,
			},
			want: logical.NewMap(
				[]logical.NamedExpression{
					logical.NewVariable("a.age"),
					logical.NewVariable("a.name"),
				},
				logical.NewMap(
					[]logical.NamedExpression{
						logical.NewVariable("a.age"),
						logical.NewVariable("a.name"),
					},
					logical.NewDataSource("anacondas", "a"),
					true,
				),
				false,
			),
			wantErr: false,
		},
		{
			name: "stars and columns no collisions",
			args: args{
				statement: `SELECT a.*, b.*, c.age, d.name FROM dogs d`,
			},
			want: logical.NewMap(
				[]logical.NamedExpression{
					logical.NewVariable("c.age"),
					logical.NewVariable("d.name"),
					logical.NewStarExpression("a"),
					logical.NewStarExpression("b"),
				},
				logical.NewMap(
					[]logical.NamedExpression{
						logical.NewVariable("c.age"),
						logical.NewVariable("d.name"),
					},
					logical.NewDataSource("dogs", "d"),
					true,
				),
				false,
			),
			wantErr: false,
		},
		{
			name: "simple union",
			args: args{
				`(SELECT p.id, p.name, p.surname FROM people p WHERE p.surname = 'Kowalski')
					UNION
					(SELECT * FROM admins a WHERE a.exp < 2)`,
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
							true),
					),
					false,
				),
				logical.NewMap(
					[]logical.NamedExpression{
						logical.NewStarExpression(""),
					},
					logical.NewFilter(
						logical.NewPredicate(
							logical.NewVariable("a.exp"),
							logical.LessThan,
							logical.NewConstant(2),
						),
						logical.NewMap(
							[]logical.NamedExpression{},
							logical.NewDataSource("admins", "a"),
							true),
					),
					false,
				),
			),
			wantErr: false,
		},
		{
			name: "simple union all + limit + NO offset",
			args: args{
				`(SELECT c.name, c.age FROM cities c WHERE c.age > 100)
					UNION ALL
					(SELECT p.name, p.age FROM people p WHERE p.age > 4)
					LIMIT 5`,
			},
			want: logical.NewUnionAll(
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
			wantErr: false,
		},
		{
			name: "simple limit + offset",
			args: args{
				"SELECT p.name, p.age FROM people p LIMIT 3 OFFSET 2",
			},
			want: logical.NewMap(
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
			wantErr: false,
		},
		{
			name: "simple union all",
			args: args{
				`(SELECT p2.name, p2.age FROM people p2 WHERE p2.age > 3)
					UNION ALL
					(SELECT p2.name, p2.age FROM people p2 WHERE p2.age > 4)`,
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
				`((SELECT p2.name, p2.age FROM people p2 WHERE p2.age > 3) UNION ALL (SELECT p2.name, p2.age FROM people p2 WHERE p2.age < 5))
					UNION ALL
					((SELECT p2.name, p2.age FROM people p2 WHERE p2.city > 'ciechanowo') UNION ALL (SELECT p2.name, p2.age FROM people p2 WHERE p2.city < 'wwa'))`,
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
			want: logical.NewMap(
				[]logical.NamedExpression{
					logical.NewStarExpression(""),
				},
				logical.NewFilter(
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
					logical.NewMap(
						[]logical.NamedExpression{},
						logical.NewDataSource("people", "p2"),
						true,
					),
				),
				false,
			),
			wantErr: false,
		},
		{
			name: "all relations",
			args: args{
				statement: `SELECT * FROM people p2 
							WHERE 	p2.age > 3 AND 
									p2.age = 3 AND 
									p2.age < 3 AND 
									p2.age <> 3 AND 
									p2.age != 3 AND 
									p2.age IN (SELECT * FROM people p3)`,
			},
			want: logical.NewMap(
				[]logical.NamedExpression{
					logical.NewStarExpression(""),
				},
				logical.NewFilter(
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
							logical.NewNodeExpression(
								logical.NewMap(
									[]logical.NamedExpression{
										logical.NewStarExpression(""),
									},
									logical.NewMap(
										[]logical.NamedExpression{},
										logical.NewDataSource("people", "p3"),
										true,
									),
									false,
								),
							),
						),
						"AND",
					),
					logical.NewMap(
						[]logical.NamedExpression{},
						logical.NewDataSource("people", "p2"),
						true,
					),
				),
				false,
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
							logical.NewMap(
								[]logical.NamedExpression{
									logical.NewStarExpression(""),
								},
								logical.NewMap(
									[]logical.NamedExpression{},
									logical.NewDataSource("people", "p4"),
									true,
								),
								false,
							),
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
					logical.NewJoin(
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
						execution.LEFT_JOIN,
					),
					true,
				),
				false,
			),
			wantErr: false,
		},
		{
			name: "right join 1",
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
					logical.NewJoin(
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
						execution.LEFT_JOIN,
					),
					true,
				),
				false,
			),
			wantErr: false,
		},
		{
			name: "select IN",
			args: args{
				statement: `SELECT * FROM people p WHERE p.age IN (1, 2, 3, 4) AND ('Jacob', 3) IN (SELECT * FROM people p)`,
			},
			want: logical.NewMap(
				[]logical.NamedExpression{
					logical.NewStarExpression(""),
				},
				logical.NewFilter(
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
								logical.NewMap(
									[]logical.NamedExpression{
										logical.NewStarExpression(""),
									},
									logical.NewMap(
										[]logical.NamedExpression{},
										logical.NewDataSource("people", "p"),
										true,
									),
									false,
								),
							),
						),
						"AND"),
					logical.NewMap(
						[]logical.NamedExpression{},
						logical.NewDataSource("people", "p"),
						true,
					),
				),
				false,
			),
			wantErr: false,
		},
		{
			name: "implicit group by",
			args: args{
				statement: `SELECT COUNT(DISTINCT p.name) FROM people p`,
			},
			want: logical.NewMap(
				[]logical.NamedExpression{
					logical.NewVariable("p.name_count_distinct"),
				},
				logical.NewGroupBy(
					logical.NewMap(
						[]logical.NamedExpression{
							logical.NewVariable("p.name"),
						},
						logical.NewDataSource("people", "p"),
						true,
					),
					[]logical.Expression{logical.NewConstant(true)},
					[]octosql.VariableName{"p.name"},
					[]logical.Aggregate{logical.CountDistinct},
					[]octosql.VariableName{""},
					[]logical.Trigger{},
				),
				false,
			),
			wantErr: false,
		},
		{
			name: "normal group by",
			args: args{
				statement: `SELECT COUNT(DISTINCT p.name), FIRST(p.age as myage) as firstage, p.surname, p.surname as mysurname FROM people p GROUP BY p.age, p.city TRIGGER AFTER DELAY INTERVAL 3 SECONDS, ON WATERMARK, COUNTING 5`,
			},
			want: logical.NewMap(
				[]logical.NamedExpression{
					logical.NewVariable("p.name_count_distinct"),
					logical.NewVariable("firstage"),
					logical.NewVariable("p.surname"),
					logical.NewVariable("mysurname"),
				},
				logical.NewGroupBy(
					logical.NewMap(
						[]logical.NamedExpression{
							logical.NewVariable("p.name"),
							logical.NewAliasedExpression(
								"myage",
								logical.NewVariable("p.age"),
							),
							logical.NewVariable("p.surname"),
							logical.NewAliasedExpression(
								"mysurname",
								logical.NewVariable("p.surname"),
							),
						},
						logical.NewDataSource("people", "p"),
						true,
					),
					[]logical.Expression{
						logical.NewVariable("p.age"),
						logical.NewVariable("p.city"),
					},
					[]octosql.VariableName{"p.name", "myage", "p.surname", "mysurname"},
					[]logical.Aggregate{logical.CountDistinct, logical.First, logical.First, logical.First},
					[]octosql.VariableName{"", "firstage", "p.surname", "mysurname"},
					[]logical.Trigger{
						logical.NewDelayTrigger(logical.NewConstant(time.Second * 3)),
						logical.NewWatermarkTrigger(),
						logical.NewCountingTrigger(logical.NewConstant(5)),
					},
				),
				false,
			),
			wantErr: false,
		},
		{
			name: "table valued function",
			args: args{
				statement: `SELECT * FROM func(
								arg0=>TABLE(test1),
								arg1=>"test", 
								arg2=>2, 
								arg3=> interval 2 hour,
								arg4=>DESCRIPTOR(test2.test3)) x`,
			},
			want: logical.NewMap(
				[]logical.NamedExpression{
					logical.NewStarExpression(""),
				},
				logical.NewMap(
					[]logical.NamedExpression{},
					logical.NewRequalifier("x",
						logical.NewTableValuedFunction( // test1
							"func",
							map[octosql.VariableName]logical.TableValuedFunctionArgumentValue{
								octosql.NewVariableName("arg0"): logical.NewTableValuedFunctionArgumentValueTable(logical.NewDataSource("test1", "")),
								octosql.NewVariableName("arg1"): logical.NewTableValuedFunctionArgumentValueExpression(logical.NewConstant("test")),
								octosql.NewVariableName("arg2"): logical.NewTableValuedFunctionArgumentValueExpression(logical.NewConstant(2)),
								octosql.NewVariableName("arg3"): logical.NewTableValuedFunctionArgumentValueExpression(logical.NewInterval(
									logical.NewConstant(2),
									logical.NewConstant("hour"),
								)),
								octosql.NewVariableName("arg4"): logical.NewTableValuedFunctionArgumentValueDescriptor("test2.test3"),
							},
						),
					),
					true,
				),
				false,
			),
			wantErr: false,
		},
		{
			name: "table valued function 1",
			args: args{
				statement: `WITH xtab AS (SELECT * FROM tab t), ytab AS (SELECT * FROM xtab x) SELECT * FROM ytab y`,
			},
			want: logical.NewWith(
				[]string{
					"xtab",
					"ytab",
				},
				[]logical.Node{
					logical.NewMap(
						[]logical.NamedExpression{
							logical.NewStarExpression(""),
						},
						logical.NewMap(
							[]logical.NamedExpression{},
							logical.NewDataSource("tab", "t"),
							true,
						),
						false,
					),
					logical.NewMap(
						[]logical.NamedExpression{
							logical.NewStarExpression(""),
						},
						logical.NewMap(
							[]logical.NamedExpression{},
							logical.NewDataSource("xtab", "x"),
							true,
						),
						false,
					),
				},
				logical.NewMap(
					[]logical.NamedExpression{
						logical.NewStarExpression(""),
					},
					logical.NewMap(
						[]logical.NamedExpression{},
						logical.NewDataSource("ytab", "y"),
						true,
					),
					false,
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

			got, _, err := ParseNode(statement)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseNode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got == nil && tt.want == nil {
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
