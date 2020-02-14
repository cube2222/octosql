package execution

import (
	"crypto/rand"
	"time"

	"github.com/pkg/errors"

	"github.com/oklog/ulid"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

func (id *StreamID) AsPrefix() []byte {
	return []byte("$" + id.Id + "$")
}

var inputStreamIDPrefix = []byte("$input$")

func GetRawStreamID() *StreamID {
	id := ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader)
	return &StreamID{
		Id: id.String(),
	}
}

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
