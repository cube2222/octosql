package main

import (
	"log"

	"github.com/xwb1989/sqlparser"
)

func main() {
	stmt, err := sqlparser.Parse("SELECT prefix(name, 3), age FROM (SELECT * FROM users) g")
	if err != nil {
		log.Println(err)
	}

	log.Println(stmt)

	if typed, ok := stmt.(*sqlparser.Select); ok {
		log.Println(typed)
		log.Println(typed)
	}

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

}
