package execution

import (
	"encoding/json"
	"reflect"
	"strconv"
	"time"
)

func ParseType(text string) interface{} {
	integer, err := strconv.ParseInt(text, 10, 64)
	if err == nil {
		return int(integer)
	}

	float, err := strconv.ParseFloat(text, 64)
	if err == nil {
		return float
	}

	boolean, err := strconv.ParseBool(text)
	if err == nil {
		return boolean
	}

	var jsonObject map[string]interface{}
	err = json.Unmarshal([]byte(text), &jsonObject)
	if err == nil {
		return NormalizeType(jsonObject)
	}

	t, err := time.Parse(time.RFC3339, text)
	if err == nil {
		return t
	}

	return text
}

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
	default:
		return value
	}
}

func AreEqual(left, right interface{}) bool {
	if reflect.TypeOf(left) != reflect.TypeOf(right) {
		return false
	}
	switch left := left.(type) {
	case int:
		right := right.(int)
		return left == right

	case float64:
		right := right.(float64)
		return left == right

	case bool:
		right := right.(bool)
		return left == right

	case string:
		right := right.(string)
		return left == right

	case time.Time:
		right := right.(time.Time)
		return left.Equal(right)

	case []interface{}:
		right := right.([]interface{})
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
		right := right.(map[string]interface{})
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
		// albo rekord, albo jesli
		right := right.(*Record)
		leftFields := left.Fields()
		rightFields := right.Fields()
		if len(leftFields) != len(rightFields) {
			return false
		}
		for i := range leftFields {
			if leftFields[i].Name != rightFields[i].Name {
				return false
			}
			if !AreEqual(left.Value(leftFields[i].Name), right.Value(rightFields[i].Name)) {
				return false
			}
		}
		return true

	default:
		return reflect.DeepEqual(left, right)
	}
}
