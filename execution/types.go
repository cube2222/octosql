package execution

import (
	"encoding/json"
	"log"
	"reflect"
	"strconv"
	"time"

	"github.com/cube2222/octosql"
)

type Datatype string

const (
	DatatypeBoolean Datatype = "boolean"
	DatatypeInt     Datatype = "int"
	DatatypeFloat64 Datatype = "float64"
	DatatypeString  Datatype = "string"
	DatatypeTuple   Datatype = "octosql.Tuple"
)

func GetType(i octosql.Value) Datatype {
	if _, ok := i.(octosql.Bool); ok {
		return DatatypeBoolean
	}
	if _, ok := i.(octosql.Int); ok {
		return DatatypeInt
	}
	if _, ok := i.(octosql.Float); ok {
		return DatatypeFloat64
	}
	if _, ok := i.(octosql.String); ok {
		return DatatypeString
	}
	if _, ok := i.(octosql.Tuple); ok {
		return DatatypeTuple
	}
	return DatatypeString // TODO: Unknown
}

// ParseType tries to parse the given string into any type it succeeds to. Returns back the string on failure.
func ParseType(str string) octosql.Value {
	integer, err := strconv.ParseInt(str, 10, 64)
	if err == nil {
		return octosql.MakeInt(int(integer))
	}

	float, err := strconv.ParseFloat(str, 64)
	if err == nil {
		return octosql.MakeFloat(float)
	}

	boolean, err := strconv.ParseBool(str)
	if err == nil {
		return octosql.MakeBool(boolean)
	}

	var jsonObject map[string]interface{}
	err = json.Unmarshal([]byte(str), &jsonObject)
	if err == nil {
		return NormalizeType(jsonObject)
	}

	t, err := time.Parse(time.RFC3339, str)
	if err == nil {
		return octosql.MakeTime(t)
	}

	return octosql.MakeString(str)
}

// NormalizeType brings various primitive types into the type we want them to be.
// All types coming out of data sources should be already normalized this way.
func NormalizeType(value interface{}) octosql.Value {
	switch value := value.(type) {
	case bool:
		return octosql.MakeBool(value)
	case int:
		return octosql.MakeInt(value)
	case int8:
		return octosql.MakeInt(int(value))
	case int32:
		return octosql.MakeInt(int(value))
	case int64:
		return octosql.MakeInt(int(value))
	case uint8:
		return octosql.MakeInt(int(value))
	case uint32:
		return octosql.MakeInt(int(value))
	case uint64:
		return octosql.MakeInt(int(value))
	case float32:
		return octosql.MakeFloat(float64(value))
	case float64:
		return octosql.MakeFloat(value)
	case []byte:
		return octosql.MakeString(string(value))
	case string:
		return octosql.MakeString(value)
	case []interface{}:
		out := make(octosql.Tuple, len(value))
		for i := range value {
			out[i] = NormalizeType(value[i])
		}
		return out
	case map[string]interface{}:
		out := make(octosql.Object)
		for k, v := range value {
			out[k] = NormalizeType(v)
		}
		return out
	case *interface{}:
		if value != nil {
			return NormalizeType(*value)
		}
		return nil
	case time.Time:
		return octosql.MakeTime(value)
	case struct{}:
		return octosql.MakePhantom()
	case octosql.Value:
		return value
	}
	log.Fatalf("invalid type to normalize: %s", reflect.TypeOf(value).String())
	panic("unreachable")
}

// AreEqual checks the equality of the given values, returning false if the types don't match.
func AreEqual(left, right octosql.Value) bool {
	if left == nil && right == nil {
		return true
	}
	switch left := left.(type) {
	case octosql.Int:
		right, ok := right.(octosql.Int)
		if !ok {
			return false
		}
		return left == right

	case octosql.Float:
		right, ok := right.(octosql.Float)
		if !ok {
			return false
		}
		return left == right

	case octosql.Bool:
		right, ok := right.(octosql.Bool)
		if !ok {
			return false
		}
		return left == right

	case octosql.String:
		right, ok := right.(octosql.String)
		if !ok {
			return false
		}
		return left == right

	case octosql.Time:
		right, ok := right.(octosql.Time)
		if !ok {
			return false
		}
		return left.Time().Equal(right.Time())

	case octosql.Tuple:
		right, ok := right.(octosql.Tuple)
		if !ok {
			return false
		}
		if len(left) != len(right) {
			return false
		}
		for i := range left {
			if !AreEqual(left[i], right[i]) {
				return false
			}
		}
		return true

	case *Record:
		rightRecord, ok := right.(*Record)
		if !ok {
			return false
		}
		leftFields := left.Fields()
		rightFields := rightRecord.Fields()
		if len(leftFields) != len(rightFields) {
			return false
		}

		for i := range leftFields {
			if !AreEqual(left.Value(leftFields[i].Name), rightRecord.Value(rightFields[i].Name)) {
				return false
			}
		}
		return true

	default:
		return reflect.DeepEqual(left, right)
	}
}
