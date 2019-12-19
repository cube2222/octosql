package octosql

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/pkg/errors"
)

/* TODO
1) String delimiter must be something smaller than any string
2) Tuple delimiter probably as well
*/

const (
	NullIdentifier      = 1 /* Nonexistent */
	PhantomIdentifier   = 2 /* Nonexsitent */
	IntIdentifier       = 3 /* Number */
	FloatIdentifier     = 4 /* Number??? */
	BoolIdentifier      = 5 /* Bool */
	StringIdentifier    = 6 /* Until StringDelimiter */
	TimestampIdentifier = 7 /* Number */
	DurationIdentifier  = 8 /* Number */
	TupleIdentifier     = 9 /* Until TupleDelimiter */
	ObjectIdentifier    = 10
)

const (
	NumberMarshalLength      = 1 + 1 + 8 // b[0] = type, b[1] = sign, b[2:] = marshal
	BoolMarshalLength        = 1 + 1     // b[0] = type, b[1] = 0/1
	NonexistentMarshalLength = 1         // null and phantom
)

const (
	StringDelimiter = 128
	TupleDelimiter  = 129
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
		result, err := SortedUnmarshalTuple(bytes)
		if err != nil {
			return err
		}

		finalValue = MakeTuple(result)
	case ObjectIdentifier:
		panic("implement me!")

	default:
		panic("unsupported type")
	}

	*v = finalValue
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
	case TypeTuple:
		return SortedMarshalTuple(v.AsSlice())
	default:
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

/* Marshal string */
func SortedMarshalString(s string) []byte {
	bytes := make([]byte, 1)
	bytes[0] = StringIdentifier

	fullBytes := []byte(s)
	for _, b := range fullBytes {
		bytes = append(bytes, byteToTwoBytes(b)...)
	}

	bytes = append(bytes, StringDelimiter)

	return bytes
}

//TODO: fix error messages
func SortedUnmarshalString(b []byte) (string, error) {
	length := len(b)

	if length%2 != 0 || length < 2 {
		return "", errors.New("Invalid string key size")
	}

	if b[length-1] != StringDelimiter {
		return "", errors.New("Invalid byte instead of StringDelimiter at the end of string")
	}

	packedBytes := make([]byte, 0)
	for i := 1; i < length-1; i += 2 {
		packedBytes = append(packedBytes, twoBytesToByte(b[i], b[i+1]))
	}

	return string(packedBytes), nil
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

/* Marshal Tuple */
func SortedMarshalTuple(vs []Value) []byte {
	result := make([]byte, 1)
	result[0] = TupleIdentifier

	for _, v := range vs {
		vBytes := sortedMarshal(&v)
		result = append(result, vBytes...)
	}

	result = append(result, TupleDelimiter)

	return result
}

func SortedUnmarshalTuple(b []byte) ([]Value, error) {
	length := len(b)
	startIndex := 1

	if b[length-1] != TupleDelimiter {
		return nil, errors.New("Invalid byte instead of TupleDelimiter at the end of tuple")
	}

	values := make([]Value, 0)
	var value Value

	for startIndex < length-1 {
		endIndex := findLengthOfUnmarshal(b, startIndex, int(b[startIndex]))

		if endIndex <= startIndex || endIndex >= length {
			return nil, errors.New("something went wrong") //TODO: write legit error message
		}

		err := value.SortedUnmarshal(b[startIndex:endIndex])
		if err != nil {
			return nil, errors.Wrap(err, "couldn't unmarshal an element of the tuple")
		}

		values = append(values, value)
		startIndex = endIndex
	}

	return values, nil
}

/* Auxiliary functions */

func reverseByteSlice(b []byte) []byte {
	c := make([]byte, len(b))

	for i, j := 0, len(b)-1; i <= j; i, j = i+1, j-1 {
		c[i] = b[j]
		c[j] = b[i]
	}

	return c
}

func byteToTwoBytes(b byte) []byte {
	/* b = x * 128 + y 0 <= y < 128 */

	return []byte{b / 128, b % 128}
}

func twoBytesToByte(x, y byte) byte {
	return 128*x + y
}

func findLengthOfUnmarshal(b []byte, startIndex, identifier int) int {
	switch identifier {
	case NullIdentifier, PhantomIdentifier:
		return startIndex + NonexistentMarshalLength
	case IntIdentifier, FloatIdentifier, TimestampIdentifier, DurationIdentifier:
		return startIndex + NumberMarshalLength
	case BoolIdentifier:
		return startIndex + BoolIdentifier
	case StringIdentifier:
		return startIndex + findPositionInByteArray(b[startIndex:], StringDelimiter) + 1
	case TupleIdentifier:
		return startIndex + findPositionInByteArray(b[startIndex:], TupleDelimiter) + 1
	default: //TODO: add Object
		panic("Unknown type")
	}
}

func findPositionInByteArray(b []byte, value byte) int {
	for i := 0; i < len(b); i++ {
		if b[i] == value {
			return i
		}
	}

	return -1
}
