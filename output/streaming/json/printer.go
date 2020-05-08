package json

import (
	"bufio"
	"encoding/json"
	"log"
	"os"

	"github.com/cube2222/octosql/execution"
)

func JSONPrinter() func(rec *execution.Record) {
	w := bufio.NewWriter(os.Stdout)
	enc := json.NewEncoder(w)

	return func(rec *execution.Record) {
		kvs := make(map[string]interface{})
		for _, field := range rec.ShowFields() {
			kvs[field.Name.String()] = rec.Value(field.Name).ToRawValue()
		}

		if err := enc.Encode(kvs); err != nil {
			log.Println("error encoding record for output print: ", err)
		}
		if err := w.Flush(); err != nil {
			log.Println("error flushing stdout buffered writer: ", err)
		}
	}
}
