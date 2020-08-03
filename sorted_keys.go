package octosql

import (
	"encoding/binary"
	"math"
	"sort"
	"time"

	"github.com/pkg/errors"
)

// Delimiters must be less than 128 and different from identifiers
// Identifiers must all be unique and less than 128
const (
	NullIdentifier      = 1  /* Nonexistent */
	PhantomIdentifier   = 2  /* Nonexistent */
	IntIdentifier       = 3  /* Number */
	FloatIdentifier     = 4  /* Number */
	BoolIdentifier      = 5  /* Bool */
	StringIdentifier    = 6  /* Until StringDelimiter */
	TimestampIdentifier = 7  /* Number */
	DurationIdentifier  = 8  /* Number */
	TupleIdentifier     = 9  /* Until TupleDelimiter */
	ObjectIdentifier    = 10 /* Object is a tuple */
	StringDelimiter     = 11
	TupleDelimiter      = 12
)

const (
	NumberMarshalLength      = 1 + 8 // b[0] = type, b[1:] = marshal
	BoolMarshalLength        = 1 + 1 // b[0] = type, b[1] = 0/1
	NonexistentMarshalLength = 1     // null and phantom
)

const (
	BYTE_OFFSET         = 128
	MinimalTupleLength  = 1 + 1 + 1 // b[0] = type, b[1] = element, b[2] = end of tuple
	MinimalStringLength = 1 + 1     // b[0] = type, b[1] = end of string
)

func (v *Value) MonotonicMarshal() []byte {
	return monotonicMarshal(v)
}

//monotonically decreasing version of MonotonicMarshal
func (v *Value) ReversedMonotonicMarshal() []byte {
	marshal := monotonicMarshal(v)
	var reversedMarshal []byte
	for _, x := range marshal {
		reversedMarshal = append(reversedMarshal, x ^ 255)
	}
	//add max byte to prevent errors with comparing when one value is prefix of another
	reversedMarshal = append(reversedMarshal, 255)
	return reversedMarshal
}

func (v *Value) MonotonicUnmarshal(bytes []byte) error {
	if len(bytes) == 0 {
		return errors.New("empty byte slice given to unmarshal")
	}

	identifier := int(bytes[0])

	var finalValue Value

	switch identifier {
	case NullIdentifier:
		err := MonotonicUnmarshalNull(bytes)
		if err != nil {
			return err
		}

		finalValue = MakeNull()
	case PhantomIdentifier:
		err := MonotonicUnmarshalPhantom(bytes)
		if err != nil {
			return err
		}

		finalValue = MakePhantom()
	case IntIdentifier:
		result, err := MonotonicUnmarshalInt64(bytes)
		if err != nil {
			return err
		}

		finalValue = MakeInt(int(result))
	case FloatIdentifier:
		result, err := MonotonicUnmarshalFloat(bytes)
		if err != nil {
			return err
		}

		finalValue = MakeFloat(result)
	case BoolIdentifier:
		result, err := MonotonicUnmarshalBool(bytes)
		if err != nil {
			return err
		}

		finalValue = MakeBool(result)
	case StringIdentifier:
		result, err := MonotonicUnmarshalString(bytes)
		if err != nil {
			return err
		}

		finalValue = MakeString(result)
	case TimestampIdentifier:
		result, err := MonotonicUnmarshalTime(bytes)
		if err != nil {
			return err
		}

		finalValue = MakeTime(result)
	case DurationIdentifier:
		result, err := MonotonicUnmarshalDuration(bytes)
		if err != nil {
			return err
		}

		finalValue = MakeDuration(result)
	case TupleIdentifier:
		result, err := MonotonicUnmarshalTuple(bytes)
		if err != nil {
			return err
		}

		finalValue = MakeTuple(result)
	case ObjectIdentifier:
		result, err := UnmarshalObject(bytes)
		if err != nil {
			return err
		}

		finalValue = MakeObject(result)
	default:
		panic("unsupported type")
	}

	*v = finalValue
	return nil
}

func monotonicMarshal(v *Value) []byte {
	switch v.GetType() {
	case TypeString:
		return MonotonicMarshalString(v.AsString())
	case TypeInt:
		return MonotonicMarshalInt64(int64(v.AsInt()))
	case TypeBool:
		return MonotonicMarshalBool(v.AsBool())
	case TypeNull:
		return MonotonicMarshalNull()
	case TypePhantom:
		return MonotonicMarshalPhantom()
	case TypeTime:
		return MonotonicMarshalTime(v.AsTime())
	case TypeDuration:
		return MonotonicMarshalDuration(v.AsDuration())
	case TypeFloat:
		return MonotonicMarshalFloat(v.AsFloat())
	case TypeTuple:
		return MonotonicMarshalTuple(v.AsSlice())
	case TypeObject:
		return MarshalObject(v.AsMap())
	default:
		panic("unknown type!")
	}
}

/* Marshal null */
func MonotonicMarshalNull() []byte {
	return []byte{NullIdentifier}
}

func MonotonicUnmarshalNull(b []byte) error {
	if len(b) != NonexistentMarshalLength {
		return errors.New("incorrect null key size")
	}

	if b[0] != NullIdentifier {
		return errors.New("incorrect null key value")
	}

	return nil
}

/* Marshal phantom */
func MonotonicMarshalPhantom() []byte {
	return []byte{PhantomIdentifier}
}

func MonotonicUnmarshalPhantom(b []byte) error {
	if len(b) != NonexistentMarshalLength {
		return errors.New("incorrect null key size")
	}

	if b[0] != PhantomIdentifier {
		return errors.New("incorrect null key value")
	}

	return nil
}

/* Marshal int and int64 */
func MonotonicMarshalInt64(i int64) []byte {
	b := make([]byte, NumberMarshalLength)
	b[0] = IntIdentifier
	binary.BigEndian.PutUint64(b[1:], uint64(i^math.MinInt64))
	return b
}

func MonotonicUnmarshalInt64(b []byte) (int64, error) {
	i := binary.BigEndian.Uint64(b[1:])
	return int64(i) ^ math.MinInt64, nil
}

/*	Marshal float
	Based on the code from https://github.com/danburkert/bytekey
*/
func MonotonicMarshalFloat(f float64) []byte {
	val := math.Float64bits(f)
	i := int(val)
	t := (i >> 63) | math.MinInt64

	b := make([]byte, NumberMarshalLength)
	b[0] = FloatIdentifier
	binary.BigEndian.PutUint64(b[1:], uint64(i^t))
	return b
}

func MonotonicUnmarshalFloat(b []byte) (float64, error) {
	val := binary.BigEndian.Uint64(b[1:])
	i := int64(val)

	t := ((i ^ math.MinInt64) >> 63) | math.MinInt64
	floatValue := math.Float64frombits(uint64(i ^ t))

	return floatValue, nil
}

/* Marshal bool */
func MonotonicMarshalBool(b bool) []byte {
	bytes := make([]byte, BoolMarshalLength)
	bytes[0] = byte(BoolIdentifier)

	if b {
		bytes[1] = 1
	} else {
		bytes[1] = 0
	}

	return bytes
}

func MonotonicUnmarshalBool(b []byte) (bool, error) {
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
func MonotonicMarshalString(s string) []byte {
	bytes := make([]byte, 1)
	bytes[0] = StringIdentifier

	fullBytes := []byte(s)
	for _, b := range fullBytes {
		bytes = append(bytes, byteToTwoBytes(b)...)
	}

	bytes = append(bytes, StringDelimiter)

	return bytes
}

func MonotonicUnmarshalString(b []byte) (string, error) {
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
func MonotonicMarshalTime(t time.Time) []byte {
	bytes := make([]byte, 2*NumberMarshalLength)

	bytes[0] = TimestampIdentifier
	seconds := MonotonicMarshalInt64(t.Unix())
	copy(bytes[1:NumberMarshalLength], seconds[1:NumberMarshalLength]) // Omit integer identifier

	bytes[9] = TimestampIdentifier
	nanoseconds := MonotonicMarshalInt64(int64(t.Nanosecond()))
	copy(bytes[NumberMarshalLength+1:2*NumberMarshalLength], nanoseconds[1:NumberMarshalLength]) // Omit integer identifier

	return bytes
}

func MonotonicUnmarshalTime(b []byte) (time.Time, error) {
	// MonotonicUnmarshalInt64 ignores the first byte.
	seconds, err := MonotonicUnmarshalInt64(b[:NumberMarshalLength])
	if err != nil {
		return time.Now(), errors.Wrap(err, "incorrect seconds representation")
	}

	nanoseconds, err := MonotonicUnmarshalInt64(b[NumberMarshalLength : 2*NumberMarshalLength])
	if err != nil {
		return time.Now(), errors.Wrap(err, "incorrect nanoseconds representation")
	}

	return time.Unix(seconds, nanoseconds).UTC(), nil
}

/* Marshal Duration */
func MonotonicMarshalDuration(d time.Duration) []byte {
	bytes := MonotonicMarshalInt64(d.Nanoseconds())
	bytes[0] = DurationIdentifier

	return bytes
}

func MonotonicUnmarshalDuration(b []byte) (time.Duration, error) {
	value, err := MonotonicUnmarshalInt64(b)
	if err != nil {
		return time.Duration(0), errors.Wrap(err, "incorrect duration key representation")
	}

	return time.Duration(value), nil
}

/* Marshal Tuple */
func MonotonicMarshalTuple(vs []Value) []byte {
	result := make([]byte, 1)
	result[0] = TupleIdentifier

	for _, v := range vs {
		vBytes := monotonicMarshal(&v)
		result = append(result, vBytes...)
	}

	result = append(result, TupleDelimiter)

	return result
}

func MonotonicUnmarshalTuple(b []byte) ([]Value, error) {
	values, _, err := recursiveMonotonicUnmarshalTuple(b)
	return values, err
}

func recursiveMonotonicUnmarshalTuple(b []byte) ([]Value, int, error) {
	values := make([]Value, 0)
	index := 1
	length := len(b)
	if length < MinimalTupleLength {
		return nil, 0, errors.New("incorrect tuple length")
	}

	var value Value

	for index < length {
		identifier := b[index]

		if identifier == TupleDelimiter {
			return values, index + 1, nil // this is +1 because we return length and index starts at 0
		}

		if identifier == TupleIdentifier {
			tupleValues, tupleLength, err := recursiveMonotonicUnmarshalTuple(b[index:])
			if err != nil {
				return nil, 0, err
			}

			values = append(values, MakeTuple(tupleValues))
			index += tupleLength
		} else if identifier == ObjectIdentifier {
			tupleValues, tupleLength, err := recursiveMonotonicUnmarshalTuple(b[index+1:])
			if err != nil {
				return nil, 0, err
			}

			object, err := tupleToObject(tupleValues)
			if err != nil {
				return nil, 0, err
			}

			values = append(values, MakeObject(object))
			index += tupleLength + 1 // +1 here because of ObjectIdentifier
		} else {
			elementLength, err := getMarshalLength(b[index:])
			if err != nil {
				return nil, 0, errors.Wrap(err, "couldn't find length of next element to unmarshal")
			}

			err = value.MonotonicUnmarshal(b[index : index+elementLength])
			if err != nil {
				return nil, 0, errors.Wrap(err, "couldn't unmarshal next element in tuple")
			}

			values = append(values, value)

			index += elementLength
		}
	}

	return nil, 0, errors.New("the last element of the tuple wasn't a delimiter")
}

/* Marshal Object */

func MarshalObject(o map[string]Value) []byte {
	values := make([]Value, 0)
	keys := sortedMapKeys(o)

	for _, key := range keys {
		values = append(values, MakeString(key))
		values = append(values, o[key])
	}

	bytes := make([]byte, 1)
	bytes[0] = ObjectIdentifier
	bytes = append(bytes, MonotonicMarshalTuple(values)...)

	return bytes
}

func UnmarshalObject(b []byte) (map[string]Value, error) {
	tuple, err := MonotonicUnmarshalTuple(b[1:])
	if err != nil {
		return nil, errors.Wrap(err, "couldn't unmarshal object")
	}

	return tupleToObject(tuple)
}

/* Auxiliary functions */
func tupleToObject(tuple []Value) (map[string]Value, error) {
	if len(tuple) < 2 || len(tuple)%2 == 1 {
		return nil, errors.New("invalid object length")
	}

	result := make(map[string]Value)

	for i := 0; i < len(tuple); i += 2 {
		key := tuple[i].AsString()
		result[key] = tuple[i+1]
	}

	return result, nil
}

func sortedMapKeys(m map[string]Value) []string {
	keys := make([]string, len(m))

	index := 0
	for key := range m {
		keys[index] = key
		index++
	}

	sort.Strings(keys)

	return keys
}

func reverseByteSlice(b []byte) []byte {
	c := make([]byte, len(b))

	for i, j := 0, len(b)-1; i <= j; i, j = i+1, j-1 {
		c[i] = b[j]
		c[j] = b[i]
	}

	return c
}

func byteToTwoBytes(b byte) []byte {
	return []byte{BYTE_OFFSET + b/BYTE_OFFSET, BYTE_OFFSET + b%BYTE_OFFSET}
}

func twoBytesToByte(x, y byte) byte {
	return BYTE_OFFSET*(x-BYTE_OFFSET) + (y - BYTE_OFFSET)
}

func getMarshalLength(b []byte) (int, error) {
	identifier := b[0]

	if isConstantLengthIdentifier(identifier) {
		return getConstantMarshalLength(identifier)
	} else if identifier == StringIdentifier {
		return getStringMarshalLength(b)
	}

	return -1, errors.New("unknown type")
}

func getConstantMarshalLength(identifier byte) (int, error) {
	switch identifier {
	case NullIdentifier, PhantomIdentifier:
		return NonexistentMarshalLength, nil
	case TimestampIdentifier:
		return 2 * NumberMarshalLength, nil
	case IntIdentifier, FloatIdentifier, DurationIdentifier:
		return NumberMarshalLength, nil
	case BoolIdentifier:
		return BoolMarshalLength, nil
	}

	return -1, errors.New("given identifier doesn't represent a constant length type")
}

func getStringMarshalLength(b []byte) (int, error) {
	length := len(b)

	if length < MinimalStringLength {
		return -1, errors.New("invalid marshal string length")
	}

	if b[0] != StringIdentifier {
		return -1, errors.New("expected a string, but got some other identifier")
	}

	for index := 1; index < length; index++ {
		if b[index] == StringDelimiter {
			return index + 1, nil // this is +1 because we return length and index starts at 0
		}
	}

	return -1, errors.New("didn't find the StringDelimiter in string marshal")
}

func isConstantLengthIdentifier(identifier byte) bool {
	switch identifier {
	case NullIdentifier, PhantomIdentifier, IntIdentifier, BoolIdentifier, FloatIdentifier, TimestampIdentifier, DurationIdentifier:
		return true
	}

	return false
}
