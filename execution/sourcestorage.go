package execution

import (
	"crypto/rand"
	"time"

	"github.com/pkg/errors"

	"github.com/oklog/ulid"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

// A RecordStream node should use its StreamID as a prefix to all storage operations.
// This is a helper function to make that easier.
func (id *StreamID) AsPrefix() []byte {
	return []byte("$" + id.Id + "$")
}

var inputStreamIDPrefix = []byte("$input$")

// GetRawStreamID can be used to get a new StreamID without saving it.
func GetRawStreamID() *StreamID {
	id := ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader)
	return &StreamID{
		Id: id.String(),
	}
}

// GetSourceStreamID loads the StreamID of the given input stream in case it exists (from a previous run maybe?)
// Otherwise it allocates a new StreamID and saves it.
func GetSourceStreamID(tx storage.StateTransaction, inputName octosql.Value) (*StreamID, error) {
	sourceStreamMap := storage.NewMap(tx.WithPrefix(inputStreamIDPrefix))

	var streamID StreamID
	err := sourceStreamMap.Get(&inputName, &streamID)
	if err == storage.ErrNotFound {
		streamID = *GetRawStreamID()

		err := sourceStreamMap.Set(&inputName, &streamID)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't set new stream id")
		}
	} else if err != nil {
		return nil, errors.Wrap(err, "couldn't get current value for stream id")
	}

	return &streamID, nil
}
