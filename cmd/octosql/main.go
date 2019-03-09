package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/storage/json"
)

func main() {
	/*stmt, err := sqlparser.Parse("SELECT prefix(name, 3), age FROM (SELECT * FROM users) g")
	if err != nil {
		log.Println(err)
	}

	log.Println(stmt)

	if typed, ok := stmt.(*sqlparser.Select); ok {
		log.Println(typed)
		log.Println(typed)
	}*/

	/*client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pipe := client.Pipeline()
	pipe.HSet("Jan", "Surname", "Chomik")
	pipe.HSet("Jan", "Age", 5)
	status := pipe.Save()
	log.Println(status.Err())*/

	/*status := client.HMSet("Wojciech", map[string]interface{}{
		"Surname": "Kuźminski",
		"Age": "6",
	})
	log.Println(status.Err())*/

	/*res := client.HGetAll("Jan")
	result, err := res.Result()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%+v", result)*/

	/*desc := json.NewJSONDataSourceDescription("people.json")
	ds, err := desc.Initialize(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}
	records, err := ds.Get(nil)
	if err != nil {
		log.Fatal(err)
	}
	var record *octosql.Record
	for record, err = records.Next(); err == nil; record, err = records.Next() {
		log.Printf("%+v", record.Fields())
		log.Printf("%+v", record.Value("city"))
		poch := record.Value("pochodzenie")
		if poch != nil {
			log.Printf("%+v", poch.([]interface{})[0])
		}
	}
	if err != nil {
		log.Fatal(err)
	}*/
	var record *execution.Record
	ctx := context.Background()

	dataSourceRespository := physical.NewDataSourceRepository()
	err := dataSourceRespository.Register("people", json.NewDataSourceBuilderFactory("people.json"))
	if err != nil {
		log.Fatal(err)
	}

	// ************************* przyklad 1

	// SELECT name, city FROM people WHERE age > 3
	logicalPlan := logical.NewMap(
		[]logical.Expression{
			logical.NewVariable("people.name"),
			logical.NewVariable("people.surname"),
			logical.NewVariable("people.city"),
		},
		logical.NewFilter(
			logical.NewPredicate(
				logical.NewVariable("people.age"),
				logical.NewRelation(">"),
				logical.NewConstant(3),
			),
			logical.NewDataSource("people", "people"),
		),
	)

	physicalPlan, variables, err := logicalPlan.Physical(
		ctx,
		logical.NewPhysicalPlanCreator(dataSourceRespository),
	)
	if err != nil {
		log.Fatal(err)
	}

	executor := physicalPlan.Materialize(ctx)
	stream, err := executor.Get(variables)
	if err != nil {
		log.Fatal(err)
	}

	for record, err = stream.Next(); err == nil; record, err = stream.Next() {
		fields := make([]string, len(record.Fields()))
		for i, field := range record.Fields() {
			fields[i] = fmt.Sprintf("%s = %v", field.Name, record.Value(field.Name))
		}
		log.Printf("{ %s }", strings.Join(fields, ", "))
	}

	// ************************* przyklad 2
	log.Println("Przyklad 2:")

	// SELECT p3.name, (SELECT p1.city FROM people p1 WHERE p3.name = 'Kuba' AND p1.name = 'adam') as city
	// FROM people p3
	// WHERE (SELECT p2.age FROM people p2 WHERE p2.name = 'wojtek') > p3.age

	logicalPlan2 := logical.NewMap(
		[]logical.Expression{
			logical.NewVariable("p3.name"),
			logical.NewNodeExpression(
				"city",
				logical.NewMap(
					[]logical.Expression{logical.NewVariable("p1.city")},
					logical.NewFilter(
						logical.NewInfixOperator(
							logical.NewPredicate(
								logical.NewVariable("p3.name"),
								logical.NewRelation("="),
								logical.NewConstant("Kuba"),
							),
							logical.NewPredicate(
								logical.NewVariable("p1.name"),
								logical.NewRelation("="),
								logical.NewConstant("adam"),
							),
							"AND",
						),

						logical.NewDataSource("people", "p1"),
					),
				),
			),
		},
		logical.NewFilter(
			logical.NewPredicate(
				logical.NewNodeExpression(
					"wojtek_age",
					logical.NewMap(
						[]logical.Expression{logical.NewVariable("p2.age")},
						logical.NewFilter(
							logical.NewPredicate(
								logical.NewVariable("p2.name"),
								logical.NewRelation("="),
								logical.NewConstant("wojtek"),
							),
							logical.NewDataSource("people", "p2"),
						),
					),
				),
				logical.NewRelation(">"),
				logical.NewVariable("p3.age"),
			),
			logical.NewDataSource("people", "p3"),
		),
	)

	physicalPlan2, variables2, err := logicalPlan2.Physical(
		ctx,
		logical.NewPhysicalPlanCreator(dataSourceRespository),
	)
	if err != nil {
		log.Fatal(err)
	}

	executor2 := physicalPlan2.Materialize(ctx)
	stream2, err := executor2.Get(variables2)
	if err != nil {
		log.Fatal(err)
	}

	for record, err = stream2.Next(); err == nil; record, err = stream2.Next() {
		fields := make([]string, len(record.Fields()))
		for i, field := range record.Fields() {
			fields[i] = fmt.Sprintf("%s = %v", field.Name, record.Value(field.Name))
		}
		log.Printf("{ %s }", strings.Join(fields, ", "))
	}
}
