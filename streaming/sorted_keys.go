package streaming

import (
	"encoding/binary"

	"github.com/cube2222/octosql"
)

//TODO: Should these (especially marshals) throw errors?

func SortedMarshal(v *octosql.Value) ([]byte, error) {
	switch v.GetType() {
	case octosql.TypeString:
		return SortedMarshalString(v.AsString())
	case octosql.TypeInt:
		return SortedMarshalInt(v.AsInt())
	default: //TODO: add other types
		return nil, nil
	}
}

func SortedUnmarshal(b []byte, v *octosql.Value) error {
	switch v.GetType() {
	case octosql.TypeString:
		s, err := SortedUnmarshalString(b)
		if err != nil {
			return err
		}

		*v = octosql.MakeString(s)
		return nil
	case octosql.TypeInt:
		i, err := SortedUnmarshalInt(b)
		if err != nil {
			return err
		}

		*v = octosql.MakeInt(i)
		return nil
	default: //TODO: add other types
		return nil
	}
}

func SortedMarshalString(s string) ([]byte, error) {
	return []byte(s), nil
}

func SortedUnmarshalString(b []byte) (string, error) {
	return string(b), nil
}

func SortedMarshalInt(i int) ([]byte, error) {
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

func reverseByteSlice(b []byte) []byte {
	c := make([]byte, len(b))

	for i, j := 0, len(b)-1; i <= j; i, j = i+1, j-1 {
		c[i] = b[j]
		c[j] = b[i]
	}

	return c
}
