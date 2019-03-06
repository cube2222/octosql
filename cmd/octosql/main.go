package main

import (
	"context"
	"log"

	"github.com/cube2222/octosql"
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

	desc := json.NewJSONDataSourceDescription("people.json")
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
	}
}
