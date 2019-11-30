package streaming

import (
	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/serialization"
	"github.com/pkg/errors"
)

type LinkedList struct {
	tx           *badgerTransaction
	elementCount int
}

func NewLinkedList(tx *badgerTransaction) *LinkedList {
	return &LinkedList{
		tx:           tx,
		elementCount: 0,
	}
}

func (ll *LinkedList) Append(element []byte) error {
	err := ll.tx.Set(intToByteSlice(ll.elementCount), element)
	if err != nil {
		return errors.Wrap(err, "couldn't add the element to linked list")
	}

	ll.elementCount += 1
	return nil
}

/*What about passing the deserialization method? Should this just call deserialize,
or accept a method that translates []byte to octosql.Value?
*/
func (ll *LinkedList) GetAll() ([]octosql.Value, error) {
	elements := make([]octosql.Value, ll.elementCount)

	for i := 0; i < ll.elementCount; i++ {
		el, err := ll.tx.Get(intToByteSlice(i))
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get an element of linked list")
		}

		deserialized, err := serialization.Deserialize(el)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't deserialize an element of linked list")
		}

		elements[i] = deserialized
	}

	return elements, nil
}

func intToByteSlice(x int) []byte {
	return []byte(string(x))
}
