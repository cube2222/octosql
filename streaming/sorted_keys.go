package streaming

import (
	"encoding/binary"
	"time"

	"github.com/cube2222/octosql"
)

//TODO: Should these (especially marshals) throw errors?

func SortedMarshal(v *octosql.Value) ([]byte, error) {
	switch v.GetType() {
	case octosql.TypeString:
		return SortedMarshalString(v.AsString())
	case octosql.TypeInt:
		return SortedMarshalInt(v.AsInt())
	case octosql.TypeBool:
		return SortedMarshalBool(v.AsBool())
	case octosql.TypeTime:
		return SortedMarshalTime(v.AsTime())
	default: //TODO: add other types
		return nil, nil
	}
}

func SortedUnmarshal(b []byte, v *octosql.Value) error {
	switch v.GetType() {
	case octosql.TypeString:
		u, err := SortedUnmarshalString(b)
		if err != nil {
			return err
		}

		*v = octosql.MakeString(u)
		return nil
	case octosql.TypeInt:
		u, err := SortedUnmarshalInt(b)
		if err != nil {
			return err
		}

		*v = octosql.MakeInt(u)
		return nil
	case octosql.TypeBool:
		u, err := SortedUnmarshalBool(b)
		if err != nil {
			return err
		}

		*v = octosql.MakeBool(u)
	case octosql.TypeTime:
		u, err := SortedUnmarshalTime(b)
		if err != nil {
			return err
		}

		*v = octosql.MakeTime(u)
	default: //TODO: add other types
		return nil
	}

	panic("unreachable")
}

/* Marshal string */
func SortedMarshalString(s string) ([]byte, error) {
	return []byte(s), nil
}

func SortedUnmarshalString(b []byte) (string, error) {
	return string(b), nil
}

/* Marshal int and int64 */

func SortedMarshalInt(i int) ([]byte, error) {
	return SortedMarshalInt64(int64(i))
}

func SortedMarshalInt64(i int64) ([]byte, error) {
	b := make([]byte, 9)
	binary.LittleEndian.PutUint64(b, uint64(i))

	if i >= 0 {
		b[8] = 1
	} else {
		b[8] = 0
	}

	return reverseByteSlice(b), nil
}

func SortedUnmarshalInt(b []byte) (int, error) {
	return int(binary.LittleEndian.Uint64(reverseByteSlice(b[1:]))), nil
}

func SortedUnmarshalInt64(b []byte) (int64, error) {
	return int64(binary.LittleEndian.Uint64(reverseByteSlice(b[1:]))), nil
}

func reverseByteSlice(b []byte) []byte {
	c := make([]byte, len(b))

	for i, j := 0, len(b)-1; i <= j; i, j = i+1, j-1 {
		c[i] = b[j]
		c[j] = b[i]
	}

	return c
}

/* Marshal bool */
func SortedMarshalBool(b bool) ([]byte, error) {
	if b {
		return []byte{1}, nil
	}

	return []byte{0}, nil
}

func SortedUnmarshalBool(b []byte) (bool, error) {
	if b[0] == 0 {
		return false, nil
	}

	return true, nil
}

/* Marshal Timestamp */
func SortedMarshalTime(t time.Time) ([]byte, error) {
	return SortedMarshalInt64(t.UnixNano())
}

func SortedUnmarshalTime(b []byte) (time.Time, error) {
	value, err := SortedUnmarshalInt64(b)
	if err != nil {
		return time.Now(), err
	}

	return time.Unix(value/int64(time.Second), value%int64(time.Second)), nil
}
