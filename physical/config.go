package physical

import (
	"github.com/cube2222/octosql/config"
	"github.com/pkg/errors"
)

type Factory func(dbConfig map[string]interface{}) (DataSourceBuilderFactory, error)

// CreateDataSourceRepositoryFromConfig creates a DataSourceRepository from a config,
// using the given configuration reading data source factories.
// The map should be given as databaseType -> Factory.
func CreateDataSourceRepositoryFromConfig(factories map[string]Factory, config *config.Config) (*DataSourceRepository, error) {
	repo := NewDataSourceRepository()
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
