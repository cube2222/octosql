package config

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

type DataSourceConfig struct {
	Name   string                 `yaml:"name"`
	Type   string                 `yaml:"type"`
	Config map[string]interface{} `yaml:"config"`
}

type Config struct {
	DataSources []DataSourceConfig     `yaml:"dataSources"`
	Execution   map[string]interface{} `yaml:"execution"`
	Physical    map[string]interface{} `yaml:"physical"`
}

func (config *Config) GetDataSourceConfig(name string) (map[string]interface{}, error) {
	for i := range config.DataSources {
		if config.DataSources[i].Name == name {
			return config.DataSources[i].Config, nil
		}
	}

	return nil, ErrNotFound
}

func ReadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't open file")
	}
	defer f.Close()

	var config Config

	err = yaml.NewDecoder(f).Decode(&config)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't decode yaml configuration")
	}

	for i := range config.DataSources {
		cleanupMaps(config.DataSources[i].Config)
	}

	return &config, nil
}

// The yaml decoder creates maps of type map[interface{}]interface{}.
// cleanupMaps will change them to map[string]interface{}.
func cleanupMaps(config map[string]interface{}) {
	for k, v := range config {
		config[k] = cleanupMapsRecursive(v)
	}
	return
}

func cleanupMapsRecursive(config interface{}) interface{} {
	switch config := config.(type) {
	case map[interface{}]interface{}:
		out := make(map[string]interface{})
		for k, v := range config {
			out[fmt.Sprintf("%v", k)] = cleanupMapsRecursive(v)
		}
		return out
	case []interface{}:
		for i := range config {
			config[i] = cleanupMapsRecursive(config[i])
		}
	}

	return config
}
