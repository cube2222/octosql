package config

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

func GetInterface(config map[string]interface{}, field string) (interface{}, error) {
	i := strings.Index(field, ".")
	if i == -1 {
		element, ok := config[field]
		if !ok {
			return nil, errors.Errorf("field not found %v", field)
		}
		return element, nil
	}

	element := config[field[:i]]
	submap, ok := element.(map[string]interface{})
	if !ok {
		return nil, errors.Errorf("%v should be a map, got: %v", field[:i], reflect.TypeOf(element))
	}

	out, err := GetInterface(submap, field[i+1:])
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't get interface from %v", field[i+1:])
	}

	return out, nil
}

func GetInterfaceList(config map[string]interface{}, field string) ([]interface{}, error) {
	out, err := GetInterface(config, field)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't get interface{}")
	}

	outTable, ok := out.([]interface{})
	if !ok {
		return nil, errors.Errorf("expected interface{} slice, got %v", reflect.TypeOf(out))
	}

	return outTable, nil
}

func GetMap(config map[string]interface{}, field string) (map[string]interface{}, error) {
	out, err := GetInterface(config, field)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't get interface{}")
	}

	outMap, ok := out.(map[string]interface{})
	if !ok {
		return nil, errors.Errorf("expected map, got %v", reflect.TypeOf(out))
	}

	return outMap, nil
}

func GetString(config map[string]interface{}, field string) (string, error) {
	out, err := GetInterface(config, field)
	if err != nil {
		return "", errors.Wrapf(err, "couldn't get interface{}")
	}

	outString, ok := out.(string)
	if !ok {
		return "", errors.Errorf("expected string, got %v", reflect.TypeOf(out))
	}

	return outString, nil
}

func GetInt(config map[string]interface{}, field string) (int, error) {
	out, err := GetInterface(config, field)
	if err != nil {
		return 0, errors.Wrapf(err, "couldn't get interface{}")
	}

	outInt, ok := out.(int)
	if !ok {
		return 0, errors.Errorf("expected int, got %v", reflect.TypeOf(out))
	}

	return outInt, nil
}

func GetFloat64(config map[string]interface{}, field string) (float64, error) {
	out, err := GetInterface(config, field)
	if err != nil {
		return 0, errors.Wrapf(err, "couldn't get interface{}")
	}

	outFloat, ok := out.(float64)
	if !ok {
		return 0, errors.Errorf("expected float64, got %v", reflect.TypeOf(out))
	}

	return outFloat, nil
}

func GetAddress(config map[string]interface{}, field string) (string, int, error) {
	value, err := GetString(config, field)
	if err != nil {
		return "", 0, errors.Wrapf(err, "couldn't get string")
	}

	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return "", 0, errors.New("expected address to be in host:port form")
	}

	port, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return "", 0, errors.Wrap(err, "couldn't parse port")
	}

	return parts[0], int(port), nil
}

func GetStringList(config map[string]interface{}, field string) ([]string, error) {
	out, err := GetInterfaceList(config, field)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't get []interface{}")
	}

	var outStrings []string

	for i := range out {
		outString, ok := out[i].(string)
		if !ok {
			return nil, errors.Errorf("expected string slice, got %v at index %v", reflect.TypeOf(out[i]), i)
		}
		outStrings = append(outStrings, outString)
	}

	return outStrings, nil
}
