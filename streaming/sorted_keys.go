package streaming

import (
	"encoding/binary"
	"math"
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
	case octosql.TypeBool, octosql.TypePhantom, octosql.TypeNull:
		return SortedMarshalBool(v.AsBool())
	case octosql.TypeTime:
		return SortedMarshalTime(v.AsTime())
	case octosql.TypeDuration:
		return SortedMarshalDuration(v.AsDuration())
	case octosql.TypeFloat:
		return SortedMarshalFloat(v.AsFloat())
	default: //TODO: add other types - tuple/object
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
	case octosql.TypeBool, octosql.TypePhantom, octosql.TypeNull:
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
	case octosql.TypeDuration:
		u, err := SortedUnmarshalDuration(b)
		if err != nil {
			return err
		}

		*v = octosql.MakeDuration(u)
	case octosql.TypeFloat:
		u, err := SortedUnmarshalFloat(b)
		if err != nil {
			return err
		}

		*v = octosql.MakeFloat(u)
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
	return SortedMarshalUint64(uint64(i), i >= 0), nil
}

func SortedMarshalUint64(ui uint64, sign bool) []byte {
	b := make([]byte, 9)

	binary.LittleEndian.PutUint64(b, ui)

	if sign {
		b[8] = 1
	} else {
		b[8] = 0
	}

	return reverseByteSlice(b)
}

func SortedUnmarshalInt(b []byte) (int, error) {
	return int(binary.LittleEndian.Uint64(reverseByteSlice(b[1:]))), nil
}

func SortedUnmarshalInt64(b []byte) (int64, error) {
	return int64(SortedUnmarshalUint64(b)), nil
}

func SortedUnmarshalUint64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(reverseByteSlice(b[1:]))
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

/* Marshal Duration */
func SortedMarshalDuration(d time.Duration) ([]byte, error) {
	return SortedMarshalInt64(d.Nanoseconds())
}

func SortedUnmarshalDuration(b []byte) (time.Duration, error) {
	value, err := SortedUnmarshalInt64(b)
	if err != nil {
		return time.Duration(0), err
	}

	return time.Duration(value), nil
}

/* Marshal float */
func SortedMarshalFloat(f float64) ([]byte, error) {
	val := math.Float64bits(f)

	return SortedMarshalUint64(val, f >= 0.0), nil
}

func SortedUnmarshalFloat(b []byte) (float64, error) {
	value := SortedUnmarshalUint64(b)

	return math.Float64frombits(value), nil
}
