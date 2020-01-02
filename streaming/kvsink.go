package streaming

/*
import (
	"log"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/serialization"
)

type KeyValueSink struct {
}

func (k *KeyValueSink) AddRecord(record *execution.Record, tx StateTransaction) {
	defer tx.Abort()

	err := tx.Set([]byte(record.ID().String()), serialization.Serialize(record.AsTuple()))
	if err != nil {
		log.Println("error saving record: ", err)
		return
	}
	if err = tx.Commit(); err != nil {
		log.Println("error commiting transaction: ", err)
		return
	}
}

func (k *KeyValueSink) MarkEndOfStream() error {
	return nil
}
*/
