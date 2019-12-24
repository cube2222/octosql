package streaming

/*
import (
	"context"
	"fmt"
	"log"
	"net/http"
	"testing"

	"github.com/dgraph-io/badger/v2"
	"github.com/go-chi/chi"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/serialization"
)

func TestStream(t *testing.T) {
	stream := execution.NewInMemoryStream([]*execution.Record{
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"name", "color"},
			[]interface{}{"Kuba", "red"},
			execution.WithID(execution.NewID("123")),
		),
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"name", "color"},
			[]interface{}{"Janek", "blue"},
			execution.WithID(execution.NewID("124")),
		),
		execution.NewRecordFromSliceWithNormalize(
			[]octosql.VariableName{"name", "color"},
			[]interface{}{"Wojtek", "greeeen"},
			execution.WithID(execution.NewID("121")),
		),
	})

	db, err := badger.Open(badger.DefaultOptions("test"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	engine := NewPullEngine(&KeyValueSink{}, NewBadgerStorage(db), stream)
	engine.Run(context.Background())
}

func TestReader(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("test"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	m := chi.NewMux()
	m.HandleFunc("/{id}", func(w http.ResponseWriter, r *http.Request) {
		err := db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(chi.URLParam(r, "id")))
			if err != nil {
				log.Fatal(err)
			}

			return item.Value(func(data []byte) error {
				value, err := serialization.Deserialize(data)
				if err != nil {
					log.Fatal(err)
				}

				fmt.Fprint(w, value.String())

				return nil
			})
		})
		if err != nil {
			log.Fatal(err)
		}
	})
	log.Fatal(http.ListenAndServe(":3000", m))
}
*/
