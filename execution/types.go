package execution

import (
	"encoding/json"
	"reflect"
	"strconv"
	"time"

	"github.com/cube2222/octosql"
)

type Datatype string

const (
	DatatypeBoolean Datatype = "boolean"
	DatatypeInt     Datatype = "int"
	DatatypeFloat32 Datatype = "float32"
	DatatypeFloat64 Datatype = "float64"
	DatatypeString  Datatype = "string"
)

func getType(i interface{}) Datatype {
	if _, ok := i.(bool); ok {
		return DatatypeBoolean
	}
	if _, ok := i.(int); ok {
		return DatatypeInt
	}
	if _, ok := i.(float32); ok {
		return DatatypeFloat32
	}
	if _, ok := i.(float64); ok {
		return DatatypeFloat64
	}
	if _, ok := i.(string); ok {
		return DatatypeString
	}
	return DatatypeString // TODO: Unknown
}

// ParseType tries to parse the given string into any type it succeeds to. Returns back the string on failure.
func ParseType(str string) interface{} {
	integer, err := strconv.ParseInt(str, 10, 64)
	if err == nil {
		return int(integer)
	}

	float, err := strconv.ParseFloat(str, 64)
	if err == nil {
		return float
	}

	boolean, err := strconv.ParseBool(str)
	if err == nil {
		return boolean
	}

	var jsonObject map[string]interface{}
	err = json.Unmarshal([]byte(str), &jsonObject)
	if err == nil {
		return NormalizeType(jsonObject)
	}

	t, err := time.Parse(time.RFC3339, str)
	if err == nil {
		return t
	}

	return str
}

// NormalizeType brings various primitive types into the type we want them to be.
// All types coming out of data sources should be already normalized this way.
func NormalizeType(value interface{}) interface{} {
	switch value := value.(type) {
	case int8:
		return int(value)
	case int32:
		return int(value)
	case int64:
		return int(value)
	case uint8:
		return int(value)
	case uint32:
		return int(value)
	case uint64:
		return int(value)
	case float32:
		return float64(value)
	case []byte:
		return string(value)
	case []interface{}:
		out := make([]interface{}, len(value))
		for i := range value {
			out[i] = NormalizeType(value[i])
		}
		return out
	case map[string]interface{}:
		out := make(map[string]interface{})
		for k, v := range value {
			out[k] = NormalizeType(v)
		}
		return out
	case map[octosql.VariableName]interface{}:
		out := make(map[octosql.VariableName]interface{})
		for k, v := range value {
			out[k] = NormalizeType(v)
		}
		return out
	case *interface{}:
		if value != nil {
			return NormalizeType(*value)
		}
		return nil
	default:
		return value
	}
}

// AreEqual checks the equality of the given values, returning false if the types don't match.
func AreEqual(left, right interface{}) bool {
	if left == nil && right == nil {
		return true
	}
	switch left := left.(type) {
	case int:
		right, ok := right.(int)
		if !ok {
			return false
		}
		return left == right

	case float64:
		right, ok := right.(float64)
		if !ok {
			return false
		}
		return left == right

	case bool:
		right, ok := right.(bool)
		if !ok {
			return false
		}
		return left == right

	case string:
		right, ok := right.(string)
		if !ok {
			return false
		}
		return left == right

	case time.Time:
		right, ok := right.(time.Time)
		if !ok {
			return false
		}
		return left.Equal(right)

	case []interface{}:
		right, ok := right.([]interface{})
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

	case map[string]interface{}:
		right, ok := right.(map[string]interface{})
		if !ok {
			return false
		}
		if len(left) != len(right) {
			return false
		}
		for k := range left {
			if _, ok := right[k]; !ok {
				return false
			}
		}
		for k := range right {
			if _, ok := left[k]; !ok {
				return false
			}
		}
		for k := range left {
			if !AreEqual(left[k], right[k]) {
				return false
			}
		}
		return true

	case *Record:
		rightRecord, ok := right.(*Record)
		if !ok {
			temp, ok := right.(Record)
			if !ok {
				return false
			}
			rightRecord = &temp
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
