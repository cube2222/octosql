package config

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

var ErrNotFound = errors.New("field not found")

type Option func(options *options)

type options struct {
	withDefault  bool
	defaultValue interface{}
}

func getOptions(opts ...Option) *options {
	defaultOptions := &options{
		withDefault:  false,
		defaultValue: nil,
	}

	for _, opt := range opts {
		opt(defaultOptions)
	}

	return defaultOptions
}

func WithDefault(value interface{}) Option {
	return func(options *options) {
		options.withDefault = true
		options.defaultValue = value
	}
}

// GetInterface get's the given potentially nested field irrelevant of it's type.
// This will recursively descend into submaps.
func GetInterface(config map[string]interface{}, field string, opts ...Option) (interface{}, error) {
	options := getOptions(opts...)
	i := strings.Index(field, ".")
	if i == -1 {
		element, ok := config[field]
		if options.withDefault && !ok {
			return options.defaultValue, nil
		}
		if !ok {
			return nil, ErrNotFound
		}
		return element, nil
	}

	element, ok := config[field[:i]]
	if options.withDefault && !ok {
		return options.defaultValue, nil
	}
	if !ok {
		return nil, ErrNotFound
	}
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

// GetInterfaceList gets a list from the given field.
func GetInterfaceList(config map[string]interface{}, field string, opts ...Option) ([]interface{}, error) {
	options := getOptions(opts...)
	out, err := GetInterface(config, field)
	if err != nil {
		if options.withDefault && errors.Cause(err) == ErrNotFound {
			return options.defaultValue.([]interface{}), nil
		}
		return nil, errors.Wrapf(err, "couldn't get interface{}")
	}

	if out == nil {
		return nil, nil
	}

	outTable, ok := out.([]interface{})
	if !ok {
		return nil, errors.Errorf("expected interface{} slice, got %v", reflect.TypeOf(out))
	}

	return outTable, nil
}

// GetMap gets a sub-map from the given field.
func GetMap(config map[string]interface{}, field string, opts ...Option) (map[string]interface{}, error) {
	options := getOptions(opts...)
	out, err := GetInterface(config, field)
	if err != nil {
		if options.withDefault && errors.Cause(err) == ErrNotFound {
			return options.defaultValue.(map[string]interface{}), nil
		}
		return nil, errors.Wrapf(err, "couldn't get interface{}")
	}

	outMap, ok := out.(map[string]interface{})
	if !ok {
		return nil, errors.Errorf("expected map, got %v", reflect.TypeOf(out))
	}

	return outMap, nil
}

// GetString gets a string from the given field.
func GetString(config map[string]interface{}, field string, opts ...Option) (string, error) {
	options := getOptions(opts...)
	out, err := GetInterface(config, field)
	if err != nil {
		if options.withDefault && errors.Cause(err) == ErrNotFound {
			return options.defaultValue.(string), nil
		}
		return "", errors.Wrapf(err, "couldn't get interface{}")
	}

	outString, ok := out.(string)
	if !ok {
		return "", errors.Errorf("expected string, got %v", reflect.TypeOf(out))
	}

	return outString, nil
}

// GetStringList gets a string list from the given field.
func GetStringList(config map[string]interface{}, field string, opts ...Option) ([]string, error) {
	options := getOptions(opts...)
	out, err := GetInterfaceList(config, field)
	if err != nil {
		if options.withDefault && errors.Cause(err) == ErrNotFound {
			return options.defaultValue.([]string), nil
		}
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

// GetInt gets an int from the given field.
func GetInt(config map[string]interface{}, field string, opts ...Option) (int, error) {
	options := getOptions(opts...)
	out, err := GetInterface(config, field)
	if err != nil {
		if options.withDefault && errors.Cause(err) == ErrNotFound {
			return options.defaultValue.(int), nil
		}
		return 0, errors.Wrapf(err, "couldn't get interface{}")
	}

	outInt, ok := out.(int)
	if !ok {
		return 0, errors.Errorf("expected int, got %v", reflect.TypeOf(out))
	}

	return outInt, nil
}

// GetInt gets an int from the given field.
func GetBool(config map[string]interface{}, field string, opts ...Option) (bool, error) {
	options := getOptions(opts...)
	out, err := GetInterface(config, field)
	if err != nil {
		if options.withDefault && errors.Cause(err) == ErrNotFound {
			return options.defaultValue.(bool), nil
		}
		return false, errors.Wrapf(err, "couldn't get interface{}")
	}

	outBool, ok := out.(bool)
	if !ok {
		return false, errors.Errorf("expected bool, got %v", reflect.TypeOf(out))
	}

	return outBool, nil
}

// GetFloat64 gets a float64 from the given field.
func GetFloat64(config map[string]interface{}, field string, opts ...Option) (float64, error) {
	options := getOptions(opts...)
	out, err := GetInterface(config, field)
	if err != nil {
		if options.withDefault && errors.Cause(err) == ErrNotFound {
			return options.defaultValue.(float64), nil
		}
		return 0, errors.Wrapf(err, "couldn't get interface{}")
	}

	outFloat, ok := out.(float64)
	if !ok {
		return 0, errors.Errorf("expected float64, got %v", reflect.TypeOf(out))
	}

	return outFloat, nil
}

// GetIPAddress gets an ip address from the given field.
func GetIPAddress(config map[string]interface{}, field string, opts ...Option) (string, int, error) {
	options := getOptions(opts...)
	value, err := GetString(config, field)
	if err != nil {
		if options.withDefault && errors.Cause(err) == ErrNotFound {
			defaults := options.defaultValue.([]interface{})
			return defaults[0].(string), defaults[1].(int), nil
		}
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

// GetIPAddress gets an ip address list, as a list of hosts and a list of ports from the given field.
func GetIPAddressList(config map[string]interface{}, field string, opts ...Option) ([]string, []int, error) {
	options := getOptions(opts...)
	out, err := GetInterfaceList(config, field)
	if err != nil {
		if options.withDefault && errors.Cause(err) == ErrNotFound {
			defaults := options.defaultValue.([]interface{})
			return defaults[0].([]string), defaults[1].([]int), nil
		}
		return nil, nil, errors.Wrapf(err, "couldn't get []interface{}")
	}

	var outHosts []string
	var outPorts []int

	for i := range out {
		addressMap, ok := out[i].(map[string]interface{})
		if !ok {
			return nil, nil, errors.Errorf("expected map, got %v at index %v", reflect.TypeOf(out[i]), i)
		}
		hostInterface, ok := addressMap["host"]
		if !ok {
			return nil, nil, errors.Errorf("couldn't find host in map: %+v", addressMap)
		}
		host, ok := hostInterface.(string)
		if !ok {
			return nil, nil, errors.Errorf("expected host string, got %v of type %v at index %d", hostInterface, reflect.TypeOf(hostInterface), i)
		}

		portInterface, ok := addressMap["port"]
		if !ok {
			return nil, nil, errors.Errorf("couldn't find port in map: %+v", addressMap)
		}
		port, ok := portInterface.(int)
		if !ok {
			return nil, nil, errors.Errorf("expected port integer, got %v of type %v at index %d", portInterface, reflect.TypeOf(portInterface), i)
		}

		outHosts = append(outHosts, host)
		outPorts = append(outPorts, port)
	}

	return outHosts, outPorts, nil
}
