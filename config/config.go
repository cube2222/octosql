package config

import (
	"os"

	"github.com/cube2222/octosql/physical"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

type DataSourceConfig struct {
	Name   string                 `yaml:"name"`
	Type   string                 `yaml:"type"`
	Config map[string]interface{} `yaml:"config"`
}

type Config struct {
	DataSources []DataSourceConfig `yaml:"dataSources"`
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

type Factory func(dbConfig map[string]interface{}) (physical.DataSourceBuilderFactory, error)

// CreateDataSourceRepositoryFromConfig creates a DataSourceRepository from a config,
// using the given configuration reading data source factories.
// The map should be given as databaseType -> Factory.
func CreateDataSourceRepositoryFromConfig(factories map[string]Factory, config *Config) (*physical.DataSourceRepository, error) {
	repo := physical.NewDataSourceRepository()
	for _, dsConfig := range config.DataSources {
		factory, ok := factories[dsConfig.Type]
		if !ok {
			return nil, errors.Errorf("unknown data source type: %v, available: %+v", dsConfig.Type, factories)
		}
		ds, err := factory(dsConfig.Config)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't parse %v config of %v type", dsConfig.Name, dsConfig.Type)
		}
		err = repo.Register(dsConfig.Name, ds)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't register datasource")
		}
	}
	return repo, nil
}
