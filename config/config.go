package config

import (
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

	return &config, nil
}
