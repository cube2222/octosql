package storage

import (
	"github.com/pkg/errors"
)

//TODO: this file should probably have it's name changed, or have it's contents moved to some other file

var ErrNotFound = errors.New("couldn't find element")

/*
	A type that implements this interface must have a MonotonicMarshal method,
	that has the property, that if x <= y (where <= is the less-equal relation
	defined on a certain type, for example lexicographical order on strings) then
	MonotonicMarshal(x) <= MonotonicMarshal(y) (where <= is the lexicographical order on
	[]byte). Since in mathematics such a function is called Monotonous, thus the name.
	A MonotonicUnmarshal which is the inverse of MonotonicMarshal is also required.

*/
type MonotonicallySerializable interface {
	MonotonicMarshal() []byte
	MonotonicUnmarshal([]byte) error
}
