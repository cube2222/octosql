package octosql

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/pkg/errors"
)

const (
	NullIdentifier      = 1
	PhantomIdentifier   = 2
	IntIdentifier       = 3
	FloatIdentifier     = 4
	BoolIdentifier      = 5
	StringIdentifier    = 6
	TimestampIdentifier = 7
	DurationIdentifier  = 8
	TupleIdentifier     = 9
	ObjectIdentifier    = 10
)

const (
	NumberMarshalLength      = 1 + 1 + 8 // b[0] = type, b[1] = sign, b[2:] = marshal
	BoolMarshalLength        = 1 + 1     // b[0] = type, b[1] = 0/1
	NonexistentMarshalLength = 1         // null and phantom
)

func (v *Value) SortedMarshal() []byte {
	return sortedMarshal(v)
}

func (v *Value) SortedUnmarshal(bytes []byte) error {
	if len(bytes) == 0 {
		return errors.New("empty byte slice given to unmarshal")
	}

	identifier := int(bytes[0])

	var finalValue Value

	switch identifier {
	case NullIdentifier:
		err := SortedUnmarshalNull(bytes)
		if err != nil {
			return err
		}

		finalValue = MakeNull()
	case PhantomIdentifier:
		err := SortedUnmarshalPhantom(bytes)
		if err != nil {
			return err
		}

		finalValue = MakePhantom()
	case IntIdentifier:
		result, err := SortedUnmarshalInt(bytes)
		if err != nil {
			return err
		}

		finalValue = MakeInt(result)
	case FloatIdentifier:
		result, err := SortedUnmarshalFloat(bytes)
		if err != nil {
			return err
		}

		finalValue = MakeFloat(result)
	case BoolIdentifier:
		result, err := SortedUnmarshalBool(bytes)
		if err != nil {
			return err
		}

		finalValue = MakeBool(result)
	case StringIdentifier:
		result, err := SortedUnmarshalString(bytes)
		if err != nil {
			return err
		}

		finalValue = MakeString(result)
	case TimestampIdentifier:
		result, err := SortedUnmarshalTime(bytes)
		if err != nil {
			return err
		}

		finalValue = MakeTime(result)
	case DurationIdentifier:
		result, err := SortedUnmarshalDuration(bytes)
		if err != nil {
			return err
		}

		finalValue = MakeDuration(result)
	case TupleIdentifier:
		panic("implement me!")
	case ObjectIdentifier:
		panic("implement me!")

	default:
		panic("unsupported type")
	}

	v = &finalValue
	return nil
}

func sortedMarshal(v *Value) []byte {
	switch v.GetType() {
	case TypeString:
		return SortedMarshalString(v.AsString())
	case TypeInt:
		return SortedMarshalInt(v.AsInt())
	case TypeBool:
		return SortedMarshalBool(v.AsBool())
	case TypeNull:
		return SortedMarshalNull()
	case TypePhantom:
		return SortedMarshalPhantom()
	case TypeTime:
		return SortedMarshalTime(v.AsTime())
	case TypeDuration:
		return SortedMarshalDuration(v.AsDuration())
	case TypeFloat: // TODO - fix
		return SortedMarshalFloat(v.AsFloat())
	default: //TODO: add other types - tuple/object
		return nil
	}
}

/* Marshal null */
func SortedMarshalNull() []byte {
	return []byte{NullIdentifier}
}

func SortedUnmarshalNull(b []byte) error {
	if len(b) != NonexistentMarshalLength {
		return errors.New("incorrect null key size")
	}

	if b[0] != NullIdentifier {
		return errors.New("incorrect null key value")
	}

	return nil
}

/* Marshal phantom */
func SortedMarshalPhantom() []byte {
	return []byte{PhantomIdentifier}
}

func SortedUnmarshalPhantom(b []byte) error {
	if len(b) != NonexistentMarshalLength {
		return errors.New("incorrect null key size")
	}

	if b[0] != PhantomIdentifier {
		return errors.New("incorrect null key value")
	}

	return nil
}

/* Marshal string */
func SortedMarshalString(s string) []byte {
	bytes := make([]byte, 1)
	bytes[0] = StringIdentifier
	bytes = append(bytes, []byte(s)...)

	return bytes
}

func SortedUnmarshalString(b []byte) (string, error) {
	return string(b[1:]), nil
}

/* Marshal int and int64 */
func SortedMarshalInt(i int) []byte {
	return SortedMarshalUint64(uint64(i), i >= 0)
}

func SortedMarshalUint64(ui uint64, sign bool) []byte {
	b := make([]byte, NumberMarshalLength)

	binary.LittleEndian.PutUint64(b, ui)

	/* store sign of the number */
	if sign {
		b[NumberMarshalLength-2] = 1
	} else {
		b[NumberMarshalLength-2] = 0
	}

	/* store type */
	b[NumberMarshalLength-1] = IntIdentifier

	return reverseByteSlice(b)
}

func SortedUnmarshalInt(b []byte) (int, error) {
	value, err := SortedUnmarshalUint64(b)
	if err != nil {
		return 0, errors.New("incorrect int64 key size")
	}

	return int(value), nil
}

func SortedUnmarshalUint64(b []byte) (uint64, error) {
	if len(b) != NumberMarshalLength {
		return 0, errors.New("incorrect uint64 key size")
	}
	return binary.LittleEndian.Uint64(reverseByteSlice(b[2:])), nil
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
func SortedMarshalBool(b bool) []byte {
	bytes := make([]byte, BoolMarshalLength)
	bytes[0] = byte(BoolIdentifier)

	if b {
		bytes[1] = 1
	} else {
		bytes[1] = 0
	}

	return bytes
}

func SortedUnmarshalBool(b []byte) (bool, error) {
	if len(b) != BoolMarshalLength {
		return false, errors.New("incorrect bool key size")
	}

	switch b[1] {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, errors.New("incorrect bool key value")
	}
}

/* Marshal Timestamp */
func SortedMarshalTime(t time.Time) []byte {
	bytes := SortedMarshalUint64(uint64(t.UnixNano()), true)
	bytes[0] = TimestampIdentifier

	return bytes
}

func SortedUnmarshalTime(b []byte) (time.Time, error) {
	value, err := SortedUnmarshalUint64(b)
	if err != nil {
		return time.Now(), errors.Wrap(err, "incorrect time key representation")
	}

	int64Value := int64(value)

	seconds := int64Value / int64(time.Second)
	nanoseconds := int64Value % int64(time.Second)

	return time.Unix(seconds, nanoseconds), nil
}

/* Marshal Duration */
func SortedMarshalDuration(d time.Duration) []byte {
	return SortedMarshalUint64(uint64(d.Nanoseconds()), true)
}

func SortedUnmarshalDuration(b []byte) (time.Duration, error) {
	value, err := SortedUnmarshalUint64(b)
	if err != nil {
		return time.Duration(0), errors.Wrap(err, "incorrect duration key representation")
	}

	return time.Duration(value), nil
}

/* Marshal float */
func SortedMarshalFloat(f float64) []byte {
	sign := f >= 0.0

	var val uint64

	if sign {
		val = math.Float64bits(f)
	} else {
		val = math.Float64bits(math.MaxFloat64 + f)
	}

	bytes := SortedMarshalUint64(val, sign)
	bytes[0] = FloatIdentifier

	return bytes
}

func SortedUnmarshalFloat(b []byte) (float64, error) {
	value, err := SortedUnmarshalUint64(b)
	if err != nil {
		return 0.0, errors.Wrap(err, "incorrect float key representation")
	}

	floatValue := math.Float64frombits(value)
	sign := b[1]

	if sign == 0 {
		return floatValue - math.MaxFloat64, nil
	}

	return floatValue, nil
}
