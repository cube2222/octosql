package octosql

import (
	"encoding/binary"
	"math"
	"sort"
	"time"

	"github.com/pkg/errors"
)

const (
	NullIdentifier      = 1 /* Nonexistent */
	PhantomIdentifier   = 2 /* Nonexsitent */
	IntIdentifier       = 3 /* Number */
	FloatIdentifier     = 4 /* Number */
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

//Delimiters must be less than 128 and different from identifiers
const (
	StringDelimiter     = 11
	TupleDelimiter      = 12
	BYTE_OFFSET         = 128
	MinimalTupleLength  = 1 + 1 + 1 //b[0] = type, b[1] = element, b[2] = end of tuple
	MinimalStringLength = 1 + 1     // b[0] = type, b[1] = end of string
)

func (v *Value) MonotonicMarshal() []byte {
	return monotonicMarshal(v)
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
		result, err := MonotonicUnmarshalInt(bytes)
		if err != nil {
			return err
		}

		finalValue = MakeInt(result)
	case FloatIdentifier:
		result, err := UnmarshalFloat(bytes)
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
		return MonotonicMarshalInt(v.AsInt())
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
		return MarshalFloat(v.AsFloat())
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
func MonotonicMarshalInt(i int) []byte {
	return MonotonicMarshalUint64(uint64(i), i >= 0)
}

func MonotonicMarshalUint64(ui uint64, sign bool) []byte {
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

func MonotonicUnmarshalInt(b []byte) (int, error) {
	value, err := MonotonicUnmarshalUint64(b)
	if err != nil {
		return 0, errors.New("incorrect int64 key size")
	}

	return int(value), nil
}

func MonotonicUnmarshalUint64(b []byte) (uint64, error) {
	if len(b) != NumberMarshalLength {
		return 0, errors.New("incorrect uint64 key size")
	}
	return binary.LittleEndian.Uint64(reverseByteSlice(b[2:])), nil
}

/* Marshal float */
func MarshalFloat(f float64) []byte {
	val := math.Float64bits(f)

	bytes := MonotonicMarshalUint64(val, f >= 0.0)
	bytes[0] = FloatIdentifier

	return bytes
}

func UnmarshalFloat(b []byte) (float64, error) {
	value, err := MonotonicUnmarshalUint64(b)
	if err != nil {
		return 0.0, errors.Wrap(err, "incorrect float key representation")
	}

	floatValue := math.Float64frombits(value)

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
	bytes := MonotonicMarshalUint64(uint64(t.UnixNano()), true)
	bytes[0] = TimestampIdentifier

	return bytes
}

func MonotonicUnmarshalTime(b []byte) (time.Time, error) {
	value, err := MonotonicUnmarshalUint64(b)
	if err != nil {
		return time.Now(), errors.Wrap(err, "incorrect time key representation")
	}

	int64Value := int64(value)

	seconds := int64Value / int64(time.Second)
	nanoseconds := int64Value % int64(time.Second)

	return time.Unix(seconds, nanoseconds), nil
}

/* Marshal Duration */
func MonotonicMarshalDuration(d time.Duration) []byte {
	bytes := MonotonicMarshalUint64(uint64(d.Nanoseconds()), true)
	bytes[0] = DurationIdentifier

	return bytes
}

func MonotonicUnmarshalDuration(b []byte) (time.Duration, error) {
	value, err := MonotonicUnmarshalUint64(b)
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
	var value Value

	for index < length {
		identifier := b[index]

		if identifier == TupleDelimiter {
			return values, index + 1, nil //this is +1 because we return length and index starts at 0
		}

		if identifier == TupleIdentifier {
			tupleValues, tupleLength, err := recursiveMonotonicUnmarshalTuple(b[index:])
			if err != nil {
				return nil, 0, err
			}

			values = append(values, MakeTuple(tupleValues))
			index += tupleLength
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

/* Auxiliary functions */

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
	case IntIdentifier, FloatIdentifier, TimestampIdentifier, DurationIdentifier:
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
			return index + 1, nil //this is +1 because we return length and index starts at 0
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
