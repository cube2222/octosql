package config

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/mitchellh/mapstructure"
	"gopkg.in/yaml.v3"
)

type DatabaseSpecificConfig interface {
	Validate() error
}

var configTypes = map[string]func() DatabaseSpecificConfig{}

func RegisterDatabaseType(name string, constructor func() DatabaseSpecificConfig) {
	configTypes[name] = constructor
}

type Config struct {
	Databases []DatabaseConfig `yaml:"databases"`
}

type DatabaseConfig struct {
	Name   string                 `yaml:"name"`
	Type   string                 `yaml:"type"`
	Config DatabaseSpecificConfig `yaml:"config"`
}

func (d *DatabaseConfig) UnmarshalYAML(value *yaml.Node) error {
	var untypedDecoder struct {
		Name   string      `yaml:"name"`
		Type   string      `yaml:"type"`
		Config interface{} `yaml:"config"`
	}
	if err := value.Decode(&untypedDecoder); err != nil {
		return fmt.Errorf("couldn't decode database config: %w", err)
	}
	typedConfig := configTypes[untypedDecoder.Type]()
	if err := mapstructure.Decode(untypedDecoder.Config, typedConfig); err != nil {
		return fmt.Errorf("couldn't decode database-specific config for database %s: %w", untypedDecoder.Name, err)
	}
	if err := typedConfig.Validate(); err != nil {
		return fmt.Errorf("invalid database-specific config for database %s: %w", untypedDecoder.Name, err)
	}
	d.Name = untypedDecoder.Name
	d.Type = untypedDecoder.Type
	d.Config = typedConfig
	return nil
}

func Read() (*Config, error) {
	data, err := ioutil.ReadFile("octosql.yaml")
	if err != nil {
		if os.IsNotExist(err) {
			return &Config{
				Databases: nil,
			}, nil
		}
		return nil, fmt.Errorf("couldn't read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("couldn't unmarshal yaml configuration: %w", err)
	}

	return &config, nil
}
