package storage

import (
	"github.com/pkg/errors"
)

var ErrKeyNotFound = errors.New("couldn't find key")

/*
	A type that implements this interface must have a MonotonicMarshal method,
	that has the property, that if x <= y (where <= is the less-equal relation
	defined on a certain type, for example lexicographical order on strings) then
	MonotonicMarshal(x) <= MonotonicMarshal(y) (where <= is the lexicographical order on
	[]byte). Since in mathematics such a function is called Monotonous, thus the name.
	A MonotonicUnmarshal which is the inverse of MonotonicMarshal is also required.

	WARNING: Float and Object are Serializable, but the serialization is not Monotonic!
*/
type MonotonicallySerializable interface {
	MonotonicMarshal() []byte
	MonotonicUnmarshal([]byte) error
}
