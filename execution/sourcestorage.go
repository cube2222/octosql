package execution

import (
	"crypto/rand"
	"time"

	"github.com/pkg/errors"

	"github.com/oklog/ulid"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/storage"
)

// A RecordStream node should use its StreamID as a prefix to all storage operations.
// This is a helper function to make that easier.
func (id *StreamID) AsPrefix() []byte {
	return []byte("$" + id.Id + "$")
}

var inputStreamIDPrefix = []byte("$input$")

func GetRawStringID() string {
	return ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader).String()
}

// GetRawStreamID can be used to get a new random StreamID without saving it.
func GetRawStreamID() *StreamID {
	return NewStreamID(GetRawStringID())
}

// NewStreamID can be used to create a StreamID without saving it.
func NewStreamID(str string) *StreamID {
	return &StreamID{
		Id: str,
	}
}

// GetSourceStreamID loads the StreamID of the given input stream in case it exists (from a previous run maybe?)
// Otherwise it allocates a new StreamID and saves it.
func GetSourceStringID(tx storage.StateTransaction, inputName octosql.Value) (string, error) {
	sourceStreamMap := storage.NewMap(tx.WithPrefix(inputStreamIDPrefix))

	var id octosql.Value
	err := sourceStreamMap.Get(&inputName, &id)
	if err == storage.ErrNotFound {
		id = octosql.MakeString(GetRawStringID())

		err := sourceStreamMap.Set(&inputName, &id)
		if err != nil {
			return "", errors.Wrap(err, "couldn't set new id")
		}
	} else if err != nil {
		return "", errors.Wrap(err, "couldn't get current value for id")
	}

	return id.AsString(), nil
}

func GetSourceStreamID(tx storage.StateTransaction, inputName octosql.Value) (*StreamID, error) {
	id, err := GetSourceStringID(tx, inputName)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get stream id")
	}
	return NewStreamID(id), nil
}

func GetSourceShuffleID(tx storage.StateTransaction, inputName octosql.Value) (*ShuffleID, error) {
	id, err := GetSourceStringID(tx, inputName)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get stream id")
	}
	return NewShuffleID(id), nil
}
